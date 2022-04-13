let failwith fmt = Fmt.kstr failwith fmt

let pad n x =
  if String.length x > n then x else x ^ String.make (n - String.length x) ' '

let pp_header ppf (level, header) =
  let level_style =
    match level with
    | Logs.App -> Logs_fmt.app_style
    | Logs.Debug -> Logs_fmt.debug_style
    | Logs.Warning -> Logs_fmt.warn_style
    | Logs.Error -> Logs_fmt.err_style
    | Logs.Info -> Logs_fmt.info_style in
  let level = Logs.level_to_string (Some level) in
  Fmt.pf ppf "[%a][%a]"
    (Fmt.styled level_style Fmt.string)
    level (Fmt.option Fmt.string)
    (Option.map (pad 10) header)

let reporter ppf =
  let report src level ~over k msgf =
    let k _ =
      over () ;
      k () in
    let with_src_and_stamp h _ k fmt =
      let dt = Mtime.Span.to_us (Mtime_clock.elapsed ()) in
      Fmt.kpf k ppf
        ("%s %a %a: @[" ^^ fmt ^^ "@]@.")
        (pad 10 (Fmt.str "%+04.0fus" dt))
        pp_header (level, h)
        Fmt.(styled `Magenta string)
        (pad 10 @@ Logs.Src.name src) in
    msgf @@ fun ?header ?tags fmt -> with_src_and_stamp header tags k fmt in
  { Logs.report }

module SHA1 = struct
  include Digestif.SHA1

  let feed ctx ?off ?len bs = feed_bigstring ctx ?off ?len bs
  let null = digest_string ""
  let length = digest_size
  let compare a b = String.compare (to_raw_string a) (to_raw_string b)
end

module Lwt_scheduler = struct
  module Mutex = struct
    type 'a fiber = 'a Lwt.t
    type t = Lwt_mutex.t

    let create () = Lwt_mutex.create ()
    let lock t = Lwt_mutex.lock t
    let unlock t = Lwt_mutex.unlock t
  end

  module Condition = struct
    type 'a fiber = 'a Lwt.t
    type mutex = Mutex.t
    type t = unit Lwt_condition.t

    let create () = Lwt_condition.create ()
    let wait t mutex = Lwt_condition.wait ~mutex t
    let signal t = Lwt_condition.signal t ()
    let broadcast t = Lwt_condition.broadcast t ()
  end

  type 'a t = 'a Lwt.t

  let bind x f = Lwt.bind x f
  let return x = Lwt.return x
  let parallel_map ~f lst = Lwt_list.map_p f lst
  let parallel_iter ~f lst = Lwt_list.iter_p f lst
  let detach f = Lwt_preemptive.detach f ()
end

module Scheduler = Carton.Make (Lwt)
module Verify = Carton.Dec.Verify (SHA1) (Scheduler) (Lwt_scheduler)
module First_pass = Carton.Dec.Fp (SHA1)
module Idx = Carton.Dec.Idx.N (SHA1)
open Rresult

let scheduler =
  let open Lwt in
  let open Scheduler in
  {
    Carton.bind = (fun x f -> inj (bind (prj x) (fun x -> prj (f x))));
    return = (fun x -> inj (return x));
  }

let read fd buf ~off ~len = Lwt_unix.read fd buf off len |> Scheduler.inj

let replace hashtbl k v =
  try
    let v' = Hashtbl.find hashtbl k in
    if v < v' then Hashtbl.replace hashtbl k v'
  with _ -> Hashtbl.add hashtbl k v

let digest ~kind ?(off = 0) ?len buf =
  let len =
    match len with Some len -> len | None -> Bigstringaf.length buf - off in
  let ctx = SHA1.empty in
  let ctx =
    match kind with
    | `A -> SHA1.feed_string ctx (Fmt.str "commit %d\000" len)
    | `B -> SHA1.feed_string ctx (Fmt.str "tree %d\000" len)
    | `C -> SHA1.feed_string ctx (Fmt.str "blob %d\000" len)
    | `D -> SHA1.feed_string ctx (Fmt.str "tag %d\000" len) in
  let ctx = SHA1.feed_bigstring ctx ~off ~len buf in
  SHA1.get ctx

let first_pass fd =
  let ( >>= ) = scheduler.bind in
  let return = scheduler.return in

  let oc = De.bigstring_create De.io_buffer_size in
  let zw = De.make_window ~bits:15 in
  let tp = Bigstringaf.create De.io_buffer_size in
  let allocate _ = zw in
  Lwt_unix.lseek fd (SHA1.length + 8) Unix.SEEK_SET |> Scheduler.inj
  >>= fun _ ->
  First_pass.check_header scheduler read fd >>= fun (max, _, _) ->
  Lwt_unix.lseek fd (SHA1.length + 8) Unix.SEEK_SET |> Scheduler.inj
  >>= fun _ ->
  let decoder = First_pass.decoder ~o:oc ~allocate `Manual in
  let children = Hashtbl.create 0x100 in
  let where = Hashtbl.create 0x100 in
  let weight = Hashtbl.create 0x100 in
  let length = Hashtbl.create 0x100 in
  let carbon = Hashtbl.create 0x100 in
  let crcs = Hashtbl.create 0x100 in
  let matrix = Array.make max Verify.unresolved_node in

  let rec go decoder =
    match First_pass.decode decoder with
    | `Await decoder ->
        Lwt_bytes.read fd tp 0 (Bigstringaf.length tp) |> Scheduler.inj
        >>= fun len -> go (First_pass.src decoder tp 0 len)
    | `Peek decoder ->
        let keep = First_pass.src_rem decoder in
        Lwt_bytes.read fd tp keep (Bigstringaf.length tp - keep)
        |> Scheduler.inj
        >>= fun len -> go (First_pass.src decoder tp 0 (keep + len))
    | `Entry
        ({ First_pass.kind = Base _; offset; size; consumed; crc; _ }, decoder)
      ->
        let offset = Int64.add offset (Int64.of_int (SHA1.length + 8)) in
        let n = First_pass.count decoder - 1 in
        Hashtbl.add crcs offset crc ;
        Hashtbl.add weight offset size ;
        Hashtbl.add length offset size ;
        Hashtbl.add carbon offset consumed ;
        Hashtbl.add where offset n ;
        matrix.(n) <- Verify.unresolved_base ~cursor:offset ;
        go decoder
    | `Entry
        ( {
            First_pass.kind = Ofs { sub = s; source; target };
            offset;
            size;
            consumed;
            crc;
            _;
          },
          decoder ) ->
        let offset = Int64.add offset (Int64.of_int (SHA1.length + 8)) in
        let n = First_pass.count decoder - 1 in
        replace weight Int64.(sub offset (Int64.of_int s)) source ;
        replace weight offset target ;
        Hashtbl.add crcs offset crc ;
        Hashtbl.add length offset size ;
        Hashtbl.add carbon offset consumed ;
        Hashtbl.add where offset n ;
        (try
           let vs = Hashtbl.find children (`Ofs Int64.(sub offset (of_int s))) in
           Hashtbl.replace children
             (`Ofs Int64.(sub offset (of_int s)))
             (offset :: vs)
         with _ ->
           Hashtbl.add children (`Ofs Int64.(sub offset (of_int s))) [ offset ]) ;
        go decoder
    | `Entry
        ( {
            First_pass.kind = Ref { ptr; target; source };
            offset;
            size;
            consumed;
            crc;
            _;
          },
          decoder ) ->
        let offset = Int64.add offset (Int64.of_int (SHA1.length + 8)) in
        let n = First_pass.count decoder - 1 in
        replace weight offset (Stdlib.max target source) ;
        Hashtbl.add crcs offset crc ;
        Hashtbl.add length offset size ;
        Hashtbl.add carbon offset consumed ;
        Hashtbl.add where offset n ;
        (try
           let vs = Hashtbl.find children (`Ref ptr) in
           Hashtbl.replace children (`Ref ptr) (offset :: vs)
         with _ -> Hashtbl.add children (`Ref ptr) [ offset ]) ;
        go decoder
    | `End hash ->
        let where ~cursor = Hashtbl.find where cursor in
        let children ~cursor ~uid =
          match
            ( Hashtbl.find_opt children (`Ofs cursor),
              Hashtbl.find_opt children (`Ref uid) )
          with
          | Some a, Some b -> List.sort_uniq compare (a @ b)
          | Some x, None | None, Some x -> x
          | None, None -> [] in
        let weight ~cursor = Hashtbl.find weight cursor in
        let oracle = { Carton.Dec.where; children; digest; weight } in
        return (hash, matrix, oracle, crcs)
    | `Malformed err -> failwith "Block: analyze(): %s" err in
  go decoder

let map fd ~pos len =
  let fd = Lwt_unix.unix_file_descr fd in
  let max = (Unix.LargeFile.fstat fd).Unix.LargeFile.st_size in
  let len = min (Int64.sub max pos) (Int64.of_int len) in
  let len = Int64.to_int len in
  let res =
    Unix.map_file fd ~pos Bigarray.char Bigarray.c_layout false [| len |] in
  Bigarray.array1_of_genarray res

let ( >>? ) = Lwt_result.bind

let unpack fd index =
  let open Lwt.Infix in
  first_pass fd |> Scheduler.prj >>= fun (hash, matrix, oracle, crcs) ->
  Logs.debug (fun m -> m "First pass is done (hash: %a)." SHA1.pp hash) ;
  let z = De.bigstring_create De.io_buffer_size in
  let allocate bits = De.make_window ~bits in
  let never _ = assert false in
  let pack =
    Carton.Dec.make fd ~allocate ~z ~uid_ln:SHA1.length
      ~uid_rw:SHA1.of_raw_string never in
  Verify.verify ~threads:4 pack ~map ~oracle ~verbose:ignore ~matrix
  >>= fun () ->
  Logs.debug (fun m -> m "Second pass is done.") ;
  match Array.for_all Verify.is_resolved matrix with
  | false ->
      Lwt.return_error (R.msgf "Invalid image disk: the PACK file is thin.")
  | true ->
      let f status =
        let offset = Verify.offset_of_status status in
        let crc = Hashtbl.find crcs offset in
        let offset = Int64.sub offset (Int64.of_int (SHA1.length + 8)) in
        let uid = Verify.uid_of_status status in
        { Carton.Dec.Idx.crc; offset; uid } in
      let entries = Array.map f matrix in
      let o = De.bigstring_create De.io_buffer_size in
      let encoder = Idx.encoder `Manual ~pack:hash entries in
      let compare a b =
        if Bigstringaf.length a <> Bigstringaf.length b
        then false
        else
          let idx = ref 0 in
          let res = ref 0 in
          while
            !idx < Bigstringaf.length a
            &&
            (res := Char.code a.{!idx} - Char.code b.{!idx} ;
             !res = 0)
          do
            incr idx
          done ;
          !res = 0 in
      let rec go pos = function
        | (`Ok | `Partial) as continue -> (
            let fd = Lwt_unix.unix_file_descr fd in
            let len = Bigstringaf.length o - Idx.dst_rem encoder in
            let res =
              Unix.map_file fd ~pos Bigarray.char Bigarray.c_layout false
                [| len |] in
            let res = Bigarray.array1_of_genarray res in
            match compare (Bigstringaf.sub o ~off:0 ~len) res with
            | true ->
                Idx.dst encoder o 0 (Bigstringaf.length o) ;
                if continue = `Partial
                then
                  go
                    (Int64.add pos (Int64.of_int len))
                    (Idx.encode encoder `Await)
                else Lwt.return_ok ()
            | false -> Lwt.return_error (R.msgf "Invalid IDX file")) in
      Idx.dst encoder o 0 (Bigstringaf.length o) ;
      go index (Idx.encode encoder `Await) >>? fun () ->
      let index = Hashtbl.create (Array.length matrix) in
      let iter v =
        let offset = Verify.offset_of_status v in
        let hash = Verify.uid_of_status v in
        Hashtbl.add index hash offset in
      Array.iter iter matrix ;
      let pack =
        Carton.Dec.make fd ~allocate ~z ~uid_ln:SHA1.length
          ~uid_rw:SHA1.of_raw_string (Hashtbl.find index) in
      Lwt.return_ok (hash, pack)

module Commit = Git.Commit.Make (Git.Hash.Make (SHA1))

let pp_user ppf { Git.User.name; Git.User.email; _ } =
  Fmt.pf ppf "%S <%s>" name email

let show_commit commit _hash pack =
  let weight =
    Carton.Dec.weight_of_uid ~map pack ~weight:Carton.Dec.null commit in
  let raw = Carton.Dec.make_raw ~weight in
  let v = Carton.Dec.of_uid ~map pack raw commit in
  match Carton.Dec.kind v with
  | `B | `C | `D ->
      Lwt.return_error (R.msgf "Invalid Git commit object %a" SHA1.pp commit)
  | `A -> (
      let parser = Encore.to_angstrom Commit.format in
      match
        Angstrom.parse_bigstring ~consume:All parser
          (Bigstringaf.sub (Carton.Dec.raw v) ~off:0 ~len:(Carton.Dec.len v))
      with
      | Ok c ->
          Fmt.pr "%a\t: %a\n%!"
            Fmt.(styled `Blue string)
            "commit"
            Fmt.(styled `Yellow SHA1.pp)
            commit ;
          Fmt.pr "%a\t: %a\n%!"
            Fmt.(styled `Blue string)
            "author" pp_user (Commit.author c) ;
          Fmt.pr "%a\t: %a\n%!"
            Fmt.(styled `Blue string)
            "root"
            Fmt.(styled `Yellow SHA1.pp)
            (Commit.tree c) ;
          if Commit.message c <> None then Fmt.pr "\n%!" ;
          Fmt.pr "%a\n%!" Fmt.(option string) (Commit.message c) ;
          Lwt.return_ok ()
      | Error _ ->
          Lwt.return_error
            (R.msgf "Malformed Git commit object %a" SHA1.pp commit))

let main quiet filename =
  let open Lwt.Infix in
  Lwt.catch (fun () ->
      Lwt_unix.openfile (Fpath.to_string filename) Unix.[ O_RDONLY ] 0o644
      >>= fun fd ->
      Logs.debug (fun m -> m "File %a opened." Fpath.pp filename) ;
      let hdr =
        Unix.map_file
          (Lwt_unix.unix_file_descr fd)
          ~pos:0L Bigarray.char Bigarray.c_layout false
          [| SHA1.length + 8 |] in
      let hdr = Bigarray.array1_of_genarray hdr in
      let index = Bigstringaf.get_int64_le hdr SHA1.length in
      let commit = Bigstringaf.substring hdr ~off:0 ~len:SHA1.length in
      let commit = SHA1.of_raw_string commit in
      Logs.debug (fun m -> m "Index position: %016Lx." index) ;
      Logs.debug (fun m -> m "Commit: %a." SHA1.pp commit) ;
      unpack fd index >>? fun (hash, pack) ->
      match quiet with
      | true -> Lwt_unix.close fd >>= fun v -> Lwt.return_ok v
      | false ->
          show_commit commit hash pack >>= fun v ->
          Lwt_unix.close fd >>= fun () -> Lwt.return v)
  @@ fun exn ->
  Lwt.return_error (R.msgf "Internal error: %S" (Printexc.to_string exn))

let main quiet filename =
  match Lwt_main.run (main quiet filename) with
  | Ok () -> `Ok ()
  | Error (`Msg err) -> `Error (false, Fmt.str "%s." err)

let setup_logs style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer () ;
  Logs.set_level level ;
  Logs.set_reporter (reporter Fmt.stderr) ;
  let quiet = match level with Some _ -> false | None -> true in
  quiet

open Cmdliner

let filename =
  let parser str =
    match Fpath.of_string str with
    | Ok v when Sys.file_exists str -> Ok v
    | Ok v -> R.error_msgf "%a not found" Fpath.pp v
    | Error _ as err -> err in
  Arg.conv (parser, Fpath.pp)

let filename =
  let doc = "The image disk." in
  Arg.(required & pos 0 (some filename) None & info [] ~doc)

let setup_logs =
  Term.(const setup_logs $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let command =
  let doc = "Verify the given image disk." in
  Cmd.v (Cmd.info "verify" ~doc) Term.(ret (const main $ setup_logs $ filename))

let () = Cmd.(exit @@ eval command)
