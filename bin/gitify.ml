open Rresult
open Lwt.Infix

let src = Logs.Src.create "gitify" ~doc:"logs gitify's fold event"

module Log = (val Logs.src_log src : Logs.LOG)

let ( <.> ) f g x = f (g x)
let ( >>? ) = Lwt_result.bind
let failwith fmt = Fmt.kstr failwith fmt

module Fold = struct
  let rec contents ?(dotfiles = false) ?(rel = false) dir =
    let rec readdir dh acc =
      Lwt.catch
        (fun () -> Lwt_unix.readdir dh >>= Lwt.return_some)
        (fun _exn -> Lwt.return_none)
      >>= function
      | None -> Lwt.return acc
      | Some (".." | ".") -> readdir dh acc
      | Some f when dotfiles || not (f.[0] = '.') -> (
          match Fpath.of_string f with
          | Ok f -> readdir dh ((if rel then f else Fpath.(dir // f)) :: acc)
          | Error (`Msg _) -> (* ignore *) readdir dh acc)
      | Some _ -> readdir dh acc in
    Lwt.catch
      (fun () ->
        Lwt_unix.opendir (Fpath.to_string dir) >>= fun dh ->
        readdir dh [] >>= fun res ->
        Lwt_unix.closedir dh >>= fun () -> Lwt.return res)
      (function
        | Unix.Unix_error (Unix.EINTR, _, _) -> contents ~dotfiles ~rel dir
        | Unix.Unix_error (err, _, _) ->
            let err =
              Fmt.str "directory contents %a: %s" Fpath.pp dir
                (Unix.error_message err) in
            Log.err (fun m -> m "%s" err) ;
            Lwt.return []
        | exn -> Lwt.fail exn)

  let readdir =
    let readdir d = try Sys.readdir (Fpath.to_string d) with _exn -> [||] in
    Lwt.return <.> Array.to_list <.> readdir

  let rec descendant_traverse m t get add f acc =
    match t with
    | [] -> Lwt.return acc
    | x :: r ->
        if List.exists (Fpath.equal x) m
        then descendant_traverse m r get add f acc
        else
          get x >>= fun contents ->
          descendant_traverse (x :: m) (contents @ t) get add f acc
          >>= fun acc -> f x acc

  let fold ?(dotfiles = false) f acc paths =
    let process () =
      let dir_child d acc bname =
        if (not dotfiles) && bname.[0] = '.'
        then Lwt.return acc
        else Lwt.return (Fpath.(d / bname) :: acc) in
      let add stack vs = vs @ stack in
      let get path = readdir path >>= Lwt_list.fold_left_s (dir_child path) [] in
      descendant_traverse [] paths get add f acc in
    process ()

  let fold ?dotfiles f acc d = fold ?dotfiles f acc [ d ]
end

let store_error err = `Store err

let add_blob store tbl path =
  let ic = open_in (Fpath.to_string path) in
  let ln = in_channel_length ic in
  let rs = Bytes.create ln in
  really_input ic rs 0 ln ;
  close_in ic ;
  let blob = Git.Blob.of_string (Bytes.unsafe_to_string rs) in
  Git.Mem.Store.write store (Git.Value.Blob blob) >|= R.reword_error store_error
  >>? fun (hash, _) ->
  Hashtbl.add tbl path hash ;
  Lwt.return_ok hash

let add_tree store tbl path =
  let make_entry name =
    let node = Hashtbl.find tbl Fpath.(path / name) in
    let perm =
      match Unix.stat Fpath.(to_string (path / name)) with
      | { Unix.st_kind = Unix.S_REG; _ } -> `Normal
      | { Unix.st_kind = Unix.S_DIR; _ } -> `Dir
      | _ -> failwith "Invalid kind of object: %a" Fpath.pp Fpath.(path / name)
    in
    Git.Tree.entry ~name perm node in
  Fold.readdir path
  >|= List.map make_entry
  >|= ((fun x -> Git.Value.Tree x) <.> Git.Tree.v)
  >>= Git.Mem.Store.write store
  >|= R.reword_error store_error
  >>? fun (hash, _) ->
  Hashtbl.add tbl path hash ;
  Lwt.return_ok hash

let author ?date_time () =
  let ptime, tz_offset_s =
    match date_time with
    | Some (ptime, tz_offset_s) -> (ptime, tz_offset_s)
    | None -> (Ptime_clock.now (), Ptime_clock.current_tz_offset_s ()) in
  let ptime = Int64.of_float (Ptime.to_float_s ptime) in
  let tz =
    match tz_offset_s with
    | Some tz when tz < 0 ->
        let tz = abs tz in
        let hours = tz / 3600 in
        let minutes = tz mod 3600 / 60 in
        Some { Git.User.sign = `Minus; hours; minutes }
    | Some tz ->
        let hours = tz / 3600 in
        let minutes = tz mod 3600 / 60 in
        Some { Git.User.sign = `Plus; hours; minutes }
    | None -> None in
  {
    Git.User.name = "Dr. Greenthumb";
    email = "noreply@cypress.hill";
    date = (ptime, tz);
  }

let load store hash =
  Git.Mem.Store.read_inflated store hash >|= function
  | None ->
      failwith "%a not found" Digestif.SHA1.pp hash
      (* XXX(dinosaure): should not occur! *)
  | Some (`Commit, cs) -> Carton.Dec.v ~kind:`A (Cstruct.to_bigarray cs)
  | Some (`Tree, cs) -> Carton.Dec.v ~kind:`B (Cstruct.to_bigarray cs)
  | Some (`Blob, cs) -> Carton.Dec.v ~kind:`C (Cstruct.to_bigarray cs)
  | Some (`Tag, cs) -> Carton.Dec.v ~kind:`D (Cstruct.to_bigarray cs)

module Verbose = struct
  type 'a fiber = 'a Lwt.t

  let succ _ = Lwt.return_unit
  let print _ = Lwt.return_unit
end

module SHA1 = struct
  include Digestif.SHA1

  let hash x = Hashtbl.hash x
  let length = digest_size
  let compare a b = String.compare (to_raw_string a) (to_raw_string b)
  let feed = feed_bigstring
end

module Delta = Carton_lwt.Enc.Delta (SHA1) (Verbose)

let deltify store hashes =
  let fold acc hash =
    Git.Mem.Store.read_inflated store hash >>= function
    | None -> Lwt.return acc
    | Some (kind, v) ->
        let entry =
          match kind with
          | `Commit ->
              Carton.Enc.make_entry ~kind:`A ~length:(Cstruct.length v) hash
          | `Tree ->
              Carton.Enc.make_entry ~kind:`B ~length:(Cstruct.length v) hash
          | `Blob ->
              Carton.Enc.make_entry ~kind:`C ~length:(Cstruct.length v) hash
          | `Tag ->
              Carton.Enc.make_entry ~kind:`D ~length:(Cstruct.length v) hash
        in
        Lwt.return (entry :: acc) in
  Lwt_list.fold_left_s fold [] hashes >|= Array.of_list >>= fun entries ->
  Delta.delta
    ~threads:(List.init 4 (fun _ -> load store))
    ~weight:10 ~uid_ln:Digestif.SHA1.digest_size entries
  >>= fun targets -> Lwt.return_ok (entries, targets)

let header = Bigstringaf.create 12

let pack store targets stream =
  let offsets = Hashtbl.create (Array.length targets) in
  let crcs = Hashtbl.create (Array.length targets) in
  let find hash =
    match Hashtbl.find offsets hash with
    | v -> Lwt.return_some (Int64.to_int v)
    | exception Not_found -> Lwt.return_none in
  let uid =
    {
      Carton.Enc.uid_ln = SHA1.digest_size;
      Carton.Enc.uid_rw = SHA1.to_raw_string;
    } in
  let b =
    {
      Carton.Enc.o = Bigstringaf.create De.io_buffer_size;
      Carton.Enc.i = Bigstringaf.create De.io_buffer_size;
      Carton.Enc.q = De.Queue.create 0x10000;
      Carton.Enc.w = De.Lz77.make_window ~bits:15;
    } in
  let ctx = ref SHA1.empty in
  let cursor = ref 0L in
  Carton.Enc.header_of_pack ~length:(Array.length targets) header 0 12 ;
  stream (Some (Bigstringaf.to_string header)) ;
  ctx := SHA1.feed_bigstring !ctx header ~off:0 ~len:12 ;
  cursor := Int64.add !cursor 12L ;
  let encode_targets targets =
    let encode_target idx =
      Hashtbl.add offsets (Carton.Enc.target_uid targets.(idx)) !cursor ;
      Carton_lwt.Enc.encode_target ~b ~find ~load:(load store) ~uid
        targets.(idx) ~cursor:(Int64.to_int !cursor)
      >>= fun (len, encoder) ->
      let payload = Bigstringaf.substring b.o ~off:0 ~len in
      let crc =
        Checkseum.Crc32.digest_bigstring b.o 0 len Checkseum.Crc32.default in
      stream (Some payload) ;
      ctx := SHA1.feed_bigstring !ctx b.o ~off:0 ~len ;
      cursor := Int64.add !cursor (Int64.of_int len) ;
      let rec go crc encoder =
        match Carton.Enc.N.encode ~o:b.o encoder with
        | `Flush (encoder, len) ->
            let payload = Bigstringaf.substring b.o ~off:0 ~len in
            let crc = Checkseum.Crc32.digest_bigstring b.o 0 len crc in
            stream (Some payload) ;
            ctx := SHA1.feed_bigstring !ctx b.o ~off:0 ~len ;
            cursor := Int64.add !cursor (Int64.of_int len) ;
            let encoder =
              Carton.Enc.N.dst encoder b.o 0 (Bigstringaf.length b.o) in
            go crc encoder
        | `End ->
            Hashtbl.add crcs (Carton.Enc.target_uid targets.(idx)) crc ;
            Lwt.return () in
      let encoder = Carton.Enc.N.dst encoder b.o 0 (Bigstringaf.length b.o) in
      go crc encoder in
    let rec go idx =
      if idx < Array.length targets
      then encode_target idx >>= fun () -> go (succ idx)
      else Lwt.return_unit in
    go 0 in
  encode_targets targets >>= fun () ->
  let hash = SHA1.get !ctx in
  stream (Some (SHA1.to_raw_string hash)) ;
  stream None ;
  Lwt.return (hash, offsets, crcs)

let write_string fd str =
  let rec go fd str off len =
    Lwt_unix.write_string fd str off len >>= fun len' ->
    if len - len' = 0
    then Lwt.return_unit
    else go fd str (off + len') (len - len') in
  go fd str 0 (String.length str)

let write_stream cursor fd stream =
  let rec go cursor fd stream =
    Lwt_stream.get stream >>= function
    | Some str ->
        write_string fd str >>= fun () ->
        go (Int64.add cursor (Int64.of_int (String.length str))) fd stream
    | None -> Lwt.return cursor in
  go cursor fd stream

module Idx = Carton.Dec.Idx.N (SHA1)

let write_index cursor fd ~pack targets offsets crcs =
  let f target =
    let uid = Carton.Enc.target_uid target in
    let offset = Hashtbl.find offsets uid in
    let crc = Hashtbl.find crcs uid in
    { Carton.Dec.Idx.crc; offset; uid } in
  let entries = Array.map f targets in
  let encoder = Idx.encoder `Manual ~pack entries in
  let o = Bigstringaf.create De.io_buffer_size in
  let rec go cursor = function
    | (`Ok | `Partial) as state -> (
        let len = Bigstringaf.length o - Idx.dst_rem encoder in
        let str = Bigstringaf.substring o ~off:0 ~len in
        write_string fd str >>= fun () ->
        Idx.dst encoder o 0 (Bigstringaf.length o) ;
        match state with
        | `Ok -> Lwt.return (Int64.add cursor (Int64.of_int len))
        | `Partial ->
            go (Int64.add cursor (Int64.of_int len)) (Idx.encode encoder `Await)
        ) in
  Idx.dst encoder o 0 (Bigstringaf.length o) ;
  go cursor (Idx.encode encoder `Await)

let pack store hashes commit output block_size =
  let stream, pusher = Lwt_stream.create () in
  deltify store hashes >>? fun (_entries, targets) ->
  Lwt.catch (fun () ->
      Lwt_unix.openfile (Fpath.to_string output)
        Unix.[ O_WRONLY; O_CREAT; O_TRUNC ]
        0o644
      >>= fun fd ->
      write_string fd (SHA1.to_raw_string commit) >>= fun () ->
      write_string fd (String.make 8 '\000') >>= fun () ->
      (* XXX(dinosaure): [28L: = SHA1.digest_length + 8] *)
      Lwt.both (pack store targets pusher) (write_stream 28L fd stream)
      >>= fun ((pack, offsets, crcs), top) ->
      let rem = Int64.sub block_size (Int64.rem top block_size) in
      let str = String.make (Int64.to_int rem) '\000' in
      write_string fd str >>= fun () ->
      let top = Int64.add top rem in
      let index_offset = top in
      write_index top fd ~pack targets offsets crcs >>= fun top ->
      let rem = Int64.sub block_size (Int64.rem top block_size) in
      let str = String.make (Int64.to_int rem) '\000' in
      write_string fd str >>= fun () ->
      Lwt_unix.lseek fd SHA1.digest_size Unix.SEEK_SET >>= fun _ ->
      let serial = Bigstringaf.create 8 in
      Bigstringaf.set_int64_le serial 0 index_offset ;
      write_string fd (Bigstringaf.to_string serial) >>= fun () ->
      Lwt_unix.close fd >>= fun () -> Lwt.return_ok ())
  @@ fun exn ->
  Lwt.return_error (R.msgf "Internal error: %S" (Printexc.to_string exn))

let gitify ?date_time path output block_size =
  Git.Mem.Store.v (Fpath.v ".") >|= R.reword_error store_error >>? fun store ->
  let tbl = Hashtbl.create 0x100 in
  let fold path = function
    | Error _ as err -> Lwt.return err
    | Ok (_, hashes) as acc ->
    match Unix.stat (Fpath.to_string path) with
    | { Unix.st_kind = Unix.S_REG; _ } ->
        add_blob store tbl path >>? fun hash ->
        Lwt.return_ok (hash, hash :: hashes)
    | { Unix.st_kind = Unix.S_DIR; _ } ->
        add_tree store tbl path >>? fun hash ->
        Lwt.return_ok (hash, hash :: hashes)
    | _ -> Lwt.return acc in
  Fold.fold ~dotfiles:true fold (R.ok (Digestif.SHA1.digest_string "", [])) path
  >>? function
  | root, _hashes when Digestif.SHA1.equal root (Digestif.SHA1.digest_string "")
    ->
      assert false
  | root, hashes ->
      let hashes = List.sort_uniq SHA1.compare hashes in
      let author = author ?date_time () in
      let commit =
        Git.Mem.Store.Value.Commit.make ~tree:root ~author ~committer:author
          None in
      Git.Mem.Store.write store (Git.Value.Commit commit)
      >|= R.reword_error store_error
      >>? fun (hash, _) -> pack store (hash :: hashes) hash output block_size
