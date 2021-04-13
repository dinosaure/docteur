open Rresult
open Analyze

let src = Logs.Src.create "docteur.unix"

module Log = (val Logs.src_log src : Logs.LOG)

type t = {
  fd : Lwt_unix.file_descr;
  pack : (Lwt_unix.file_descr, SHA1.t) Carton.Dec.t;
  buffers : Lwt_unix.file_descr buffers Lwt_pool.t;
  directories : SHA1.t Art.t;
  files : SHA1.t Art.t;
}

let disconnect t = Lwt_unix.close t.fd

let read fd buf ~off ~len =
  let fd = Lwt_unix.unix_file_descr fd in
  let res =
    Mmap.V1.map_file fd
      ~pos:(Int64.of_int (SHA1.length + 8))
      Bigarray.char Bigarray.c_layout false [| len |] in
  let res = Bigarray.array1_of_genarray res in
  Bigstringaf.blit_to_bytes res ~src_off:0 buf ~dst_off:off ~len ;
  Scheduler.inj (Lwt.return len)

(* XXX(dinosaure): on Solo5, we do a copy. We should remove that
 * on Unix but I'm lazy to rewrite [Analyze] with such possibility. *)
let get_block fd pos buf off len =
  let fd = Lwt_unix.unix_file_descr fd in
  let res =
    Mmap.V1.map_file fd ~pos Bigarray.char Bigarray.c_layout false [| len |]
  in
  let res = Bigarray.array1_of_genarray res in
  Bigstringaf.blit res ~src_off:0 buf ~dst_off:off ~len ;
  Ok ()

type key = Mirage_kv.Key.t

type error =
  [ `Invalid_store
  | `Msg of string
  | `Dictionary_expected of key
  | `Not_found of key
  | `Value_expected of key ]

let pp_error ppf = function
  | `Invalid_store -> Fmt.pf ppf "Invalid store"
  | `Msg err -> Fmt.string ppf err
  | `Not_found key -> Fmt.pf ppf "%a not found" Mirage_kv.Key.pp key
  | `Dictionary_expected key ->
      Fmt.pf ppf "%a is not a directory" Mirage_kv.Key.pp key
  | `Value_expected key -> Fmt.pf ppf "%a is not a file" Mirage_kv.Key.pp key

let block_size = 512L

let connect ?(analyze = false) name =
  let open Lwt.Infix in
  let ( >>? ) = Lwt_result.bind in
  Log.debug (fun m -> m "connect %S" name) ;
  Lwt.catch (fun () ->
      Lwt_unix.openfile name Unix.[ O_RDONLY ] 0o644 >>= fun fd ->
      let capacity =
        (Unix.LargeFile.fstat (Lwt_unix.unix_file_descr fd))
          .Unix.LargeFile.st_size in
      Log.debug (fun m -> m "Capacity of the given image disk: %Ld" capacity) ;
      let hdr =
        Mmap.V1.map_file
          (Lwt_unix.unix_file_descr fd)
          ~pos:0L Bigarray.char Bigarray.c_layout false
          [| SHA1.length + 8 |] in
      let hdr = Bigarray.array1_of_genarray hdr in
      Log.debug (fun m -> m "Header: %S" (Bigstringaf.to_string hdr)) ;
      let commit =
        SHA1.of_raw_string (Bigstringaf.substring hdr ~off:0 ~len:SHA1.length)
      in
      let index = Bigstringaf.get_int64_le hdr SHA1.length in
      match analyze with
      | true ->
          Log.debug (fun m -> m "Start to analyze the given image disk.") ;
          unpack fd ~read ~block_size ~get_block commit
          >>? fun (buffers, pack, directories, files) ->
          Lwt.return_ok { fd; pack; buffers; directories; files }
      | false ->
          Log.debug (fun m ->
              m "Use the IDX file to reconstruct the file-system.") ;
          iter fd ~block_size ~capacity ~get_block commit index
          >>? fun (buffers, pack, directories, files) ->
          Lwt.return_ok { fd; pack; buffers; directories; files })
  @@ fun exn ->
  Lwt.return_error (R.msgf "Internal error: %s" (Printexc.to_string exn))

let map fd ~pos len =
  let fd = Lwt_unix.unix_file_descr fd in
  let max = (Unix.LargeFile.fstat fd).Unix.LargeFile.st_size in
  let len = min (Int64.of_int len) (Int64.sub max pos) in
  let len = Int64.to_int len in
  let res =
    Mmap.V1.map_file fd ~pos Bigarray.char Bigarray.c_layout false [| len |]
  in
  Bigarray.array1_of_genarray res

module Commit = Git.Commit.Make (Git.Hash.Make (SHA1))
module Tree = Git.Tree.Make (Git.Hash.Make (SHA1))

let load pack uid =
  let open Rresult in
  let weight = Carton.Dec.weight_of_uid ~map pack ~weight:Carton.Dec.null uid in
  let raw = Carton.Dec.make_raw ~weight in
  let v = Carton.Dec.of_uid ~map pack raw uid in
  match Carton.Dec.kind v with
  | `A ->
      let parser = Encore.to_angstrom Commit.format in
      Angstrom.parse_bigstring ~consume:All parser
        (Bigstringaf.sub (Carton.Dec.raw v) ~off:0 ~len:(Carton.Dec.len v))
      |> R.reword_error (fun _ -> R.msgf "Invalid commit (%a)" SHA1.pp uid)
      >>| fun v -> `Commit v
  | `B ->
      let parser = Encore.to_angstrom Tree.format in
      Angstrom.parse_bigstring ~consume:All parser
        (Bigstringaf.sub (Carton.Dec.raw v) ~off:0 ~len:(Carton.Dec.len v))
      |> R.reword_error (fun _ -> R.msgf "Invalid tree (%a)" SHA1.pp uid)
      >>| fun v -> `Tree v
  | `C ->
      R.ok
        (`Blob
          (Bigstringaf.sub (Carton.Dec.raw v) ~off:0 ~len:(Carton.Dec.len v)))
  | `D -> R.ok `Tag

let with_ressources pack uid buffers =
  Lwt.catch (fun () ->
      let pack = Carton.Dec.with_z buffers.z pack in
      let pack = Carton.Dec.with_allocate ~allocate:buffers.allocate pack in
      let pack = Carton.Dec.with_w buffers.w pack in
      load pack uid |> Lwt.return)
  @@ fun exn -> raise exn

let exists t key =
  match
    ( Art.find_opt t.directories (Art.key (Mirage_kv.Key.to_string key)),
      Art.find_opt t.files (Art.key (Mirage_kv.Key.to_string key)) )
  with
  | None, None -> Lwt.return_ok None
  | Some _, None -> Lwt.return_ok (Some `Dictionary)
  | None, Some _ -> Lwt.return_ok (Some `Value)
  | Some _, Some _ -> assert false
(* XXX(dinosaure): impossible. *)

let get t key =
  let open Rresult in
  let open Lwt.Infix in
  match Art.find_opt t.files (Art.key (Mirage_kv.Key.to_string key)) with
  | None -> Lwt.return_error (`Not_found key)
  | Some hash -> (
      Lwt_pool.use t.buffers (with_ressources t.pack hash) >>= function
      | Ok (`Blob v) -> Lwt.return_ok (Bigstringaf.to_string v)
      | Ok _ -> Lwt.return_error (`Value_expected key)
      | Error _ as err -> Lwt.return err)

let list t key =
  match Art.find_opt t.directories (Art.key (Mirage_kv.Key.to_string key)) with
  | None -> Lwt.return_error (`Not_found key)
  | Some hash -> (
      let open Lwt.Infix in
      Lwt_pool.use t.buffers (with_ressources t.pack hash) >>= function
      | Ok (`Tree v) ->
          let f acc { Git.Tree.name; perm; _ } =
            match perm with
            | `Everybody | `Normal -> (name, `Value) :: acc
            | `Dir -> (name, `Dictionary) :: acc
            | _ -> acc in
          let lst = List.fold_left f [] (Git.Tree.to_list v) in
          Lwt.return_ok lst
      | Ok _ -> Lwt.return_error (`Dictionary_expected key)
      | Error _ as err -> Lwt.return err)

let digest t key =
  match
    ( Art.find_opt t.files (Art.key (Mirage_kv.Key.to_string key)),
      Art.find_opt t.directories (Art.key (Mirage_kv.Key.to_string key)) )
  with
  | Some v, None -> Lwt.return_ok (SHA1.to_raw_string v)
  | None, Some v -> Lwt.return_ok (SHA1.to_raw_string v)
  | None, None -> Lwt.return_error (`Not_found key)
  | Some _, Some _ -> assert false

let last_modified _t _key = Lwt.return_ok (0, 0L)
