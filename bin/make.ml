open Rresult
open Lwt.Infix

let ( >>? ) = Lwt_result.bind

let () = Random.self_init ()

module Append = struct
  type 'a rd = < rd : unit ; .. > as 'a

  type 'a wr = < wr : unit ; .. > as 'a

  type 'a mode =
    | Rd : < rd : unit > mode
    | Wr : < wr : unit > mode
    | RdWr : < rd : unit ; wr : unit > mode

  type t = unit

  type uid = Fpath.t

  type 'a fd = Lwt_unix.file_descr

  type error = Unix.error * string * string

  type +'a fiber = 'a Lwt.t

  let pp_error ppf (err, f, v) =
    Fmt.pf ppf "%s(%s) : %s" f v (Unix.error_message err)

  let create :
      type a.
      ?trunc:bool -> mode:a mode -> t -> uid -> (a fd, error) result fiber =
   fun ?(trunc = true) ~mode () fpath ->
    let flags =
      match mode with
      | Rd -> Unix.[ O_RDONLY ]
      | Wr -> Unix.[ O_WRONLY; O_CREAT ]
      | RdWr -> Unix.[ O_RDWR; O_CREAT ] in
    let flags = if trunc then Unix.O_TRUNC :: flags else flags in
    let process () =
      Lwt_unix.openfile (Fpath.to_string fpath) flags 0o644 >|= R.ok in
    Lwt.catch process @@ function
    | Unix.Unix_error (err, f, g) -> Lwt.return_error (err, f, g)
    | exn -> raise exn

  let map () fd ~pos len =
    let fd = Lwt_unix.unix_file_descr fd in
    let rs =
      Mmap.V1.map_file fd ~pos Bigarray.char Bigarray.c_layout false [| len |]
    in
    Bigarray.array1_of_genarray rs

  let append () fd str =
    let rec go off len =
      Lwt_unix.write_string fd str off len >>= fun len' ->
      if len - len' = 0 then Lwt.return () else go (off + len') (len - len')
    in
    go 0 (String.length str)

  let move () ~src ~dst =
    let process () =
      Lwt_unix.rename (Fpath.to_string src) (Fpath.to_string dst) >|= R.ok in
    Lwt.catch process @@ function
    | Unix.Unix_error (err, f, v) -> Lwt.return_error (err, f, v)
    | exn -> raise exn

  let close () fd =
    let process () = Lwt_unix.close fd >|= R.ok in
    Lwt.catch process @@ function
    | Unix.Unix_error (err, f, v) -> Lwt.return_error (err, f, v)
    | exn -> raise exn
end

module Store = struct
  include Git.Mem.Store

  let batch_write _store _hash ~pck:_ ~idx:_ = Lwt.return_ok ()
end

module Sync =
  Git.Sync.Make (Digestif.SHA1) (Append) (Append) (Store) (Git_cohttp_unix)

let src = Logs.Src.create "guit-mk" ~doc:"logs binary event"

module Log = (val Logs.src_log src : Logs.LOG)

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

let setup_logs style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer () ;
  Logs.set_level level ;
  Logs.set_reporter (reporter Fmt.stderr) ;
  let quiet = match level with Some _ -> false | None -> true in
  quiet

let stream_of_file filename =
  let stream, emitter = Lwt_stream.create () in
  let fill () =
    Lwt_unix.openfile (Fpath.to_string filename) Unix.[ O_RDONLY ] 0o644
    >>= fun fd ->
    let rec go () =
      let tmp = Bytes.create 0x1000 in
      Lwt.catch (fun () ->
          Lwt_unix.read fd tmp 0 0x1000 >>= function
          | 0 ->
              emitter None ;
              Lwt_unix.close fd
          | len ->
              emitter (Some (Bytes.sub_string tmp 0 len)) ;
              go ())
      @@ fun _exn ->
      emitter None ;
      Lwt_unix.close fd in
    go () in
  Lwt.async fill ;
  fun () -> Lwt_stream.get stream

open Bos

let store_error err = `Store err

let sync_error err = `Sync err

let write_string fd str =
  let rec go fd str off len =
    Lwt_unix.write_string fd str off len >>= fun len' ->
    if len - len' = 0
    then Lwt.return_unit
    else go fd str (off + len') (len - len') in
  go fd str 0 (String.length str)

let transmit cc src dst =
  let tmp = Bytes.create 0x1000 in
  let rec go cc =
    Lwt_unix.read src tmp 0 0x1000 >>= function
    | 0 -> Lwt.return cc
    | len ->
        write_string dst (Bytes.sub_string tmp 0 len) >>= fun () ->
        go (Int64.add cc (Int64.of_int len)) in
  go cc

let save tmp hash output block_size =
  Lwt_unix.openfile (Fpath.to_string output)
    Unix.[ O_WRONLY; O_CREAT; O_TRUNC ]
    0o644
  >>= fun dst ->
  Lwt_unix.openfile (Fpath.to_string tmp) Unix.[ O_RDONLY ] 0o644 >>= fun src ->
  write_string dst (Digestif.SHA1.to_raw_string hash) >>= fun () ->
  transmit (Int64.of_int Digestif.SHA1.digest_size) src dst >>= fun top ->
  let rem = Int64.sub block_size (Int64.rem top block_size) in
  let str = String.make (Int64.to_int rem) '\000' in
  write_string dst str >>= fun () -> Lwt.return_ok ()
(* TODO(dinosaure): [Lwt.catch] *)

let fetch edn want output block_size =
  Git.Mem.Store.v (Fpath.v ".") >|= R.reword_error store_error >>? fun store ->
  OS.File.tmp "pack-%s.pack" |> Lwt.return >>? fun src ->
  OS.File.tmp "pack-%s.pack" |> Lwt.return >>? fun dst ->
  OS.File.tmp "pack-%s.idx" |> Lwt.return >>? fun idx ->
  let create_pack_stream () = stream_of_file dst in
  let create_idx_stream () = stream_of_file idx in
  Sync.fetch ~ctx:Git_unix.ctx edn store ~deepen:(`Depth 1)
    (`Some [ (want, want) ])
    ~src ~dst ~idx ~create_pack_stream ~create_idx_stream () ()
  >|= R.reword_error sync_error
  >>? function
  | Some (_, [ (_, hash) ]) -> save dst hash output block_size
  | _ -> assert false

let main _ edn want output block_size =
  match Lwt_main.run (fetch edn want output block_size) with
  | Ok () -> `Ok 0
  | Error (`Msg err) -> `Error (false, Fmt.str "%s." err)
  | Error (`Store err) -> `Error (false, Fmt.str "%a." Store.pp_error err)
  | Error (`Sync err) -> `Error (false, Fmt.str "%a." Sync.pp_error err)

open Cmdliner

let endpoint = Arg.conv (Smart_git.Endpoint.of_string, Smart_git.Endpoint.pp)

let want = Arg.conv (Git.Reference.of_string, Git.Reference.pp)

let output = Arg.conv (Fpath.of_string, Fpath.pp)

let endpoint =
  let doc = "URI leading to repository." in
  Arg.(
    required & pos 0 (some endpoint) None & info [] ~docv:"<repository>" ~doc)

let want =
  let doc = "Reference to pull." in
  Arg.(required & pos 1 (some want) None & info [] ~docv:"<reference>" ~doc)

let output =
  let doc = "Disk image." in
  Arg.(required & pos 2 (some output) None & info [] ~docv:"<filename>" ~doc)

let block_size =
  let doc = "Write up to <bytes> bytes." in
  Arg.(value & opt int64 512L & info [ "bs" ] ~docv:"<bytes>" ~doc)

let setup_logs =
  Term.(const setup_logs $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let command =
  let doc = "Fetch and make an image disk from a Git repository." in
  ( Term.(ret (const main $ setup_logs $ endpoint $ want $ output $ block_size)),
    Term.info "make" ~doc )

let () = Term.(exit @@ eval command)
