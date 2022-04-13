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
      Unix.map_file fd ~pos Bigarray.char Bigarray.c_layout false [| len |]
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

module Sync = Git.Sync.Make (Digestif.SHA1) (Append) (Append) (Store)

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

let save ~offset idx tmp hash output block_size =
  Lwt.catch (fun () ->
      Lwt_unix.openfile (Fpath.to_string output)
        Unix.[ O_WRONLY; O_CREAT; O_TRUNC ]
        0o644
      >>= fun dst ->
      Lwt_unix.openfile (Fpath.to_string tmp) Unix.[ O_RDONLY ] 0o644
      >>= fun src ->
      write_string dst (Digestif.SHA1.to_raw_string hash) >>= fun () ->
      let serial = Bigstringaf.create 8 in
      Bigstringaf.set_int64_le serial 0 offset ;
      write_string dst (Bigstringaf.to_string serial) >>= fun () ->
      transmit (Int64.of_int (Digestif.SHA1.digest_size + 8)) src dst
      >>= fun top ->
      let rem = Int64.sub block_size (Int64.rem top block_size) in
      let str = String.make (Int64.to_int rem) '\000' in
      write_string dst str >>= fun () ->
      Lwt_unix.close src >>= fun () ->
      Lwt_unix.openfile (Fpath.to_string idx) Unix.[ O_RDONLY ] 0o644
      >>= fun idx ->
      transmit (Int64.add top rem) idx dst >>= fun top ->
      let rem = Int64.sub block_size (Int64.rem top block_size) in
      let str = String.make (Int64.to_int rem) '\000' in
      write_string dst str >>= fun () ->
      Lwt_unix.close idx >>= fun () -> Lwt.return_ok ())
  @@ fun exn ->
  Lwt.return_error (R.msgf "Internal error: %S" (Printexc.to_string exn))

module SSH = struct
  type error = Unix.error * string * string
  type write_error = [ `Closed | `Error of Unix.error * string * string ]

  let pp_error ppf (err, f, v) =
    Fmt.pf ppf "%s(%s): %s" f v (Unix.error_message err)

  let pp_write_error ppf = function
    | `Closed -> Fmt.pf ppf "Connection closed by peer"
    | `Error (err, f, v) -> Fmt.pf ppf "%s(%s): %s" f v (Unix.error_message err)

  type flow = { ic : in_channel; oc : out_channel }

  type endpoint = {
    user : string;
    path : string;
    host : Unix.inet_addr;
    port : int;
  }

  let pp_inet_addr ppf inet_addr =
    Fmt.string ppf (Unix.string_of_inet_addr inet_addr)

  let connect { user; path; host; port } =
    let edn = Fmt.str "%s@%a" user pp_inet_addr host in
    let cmd = Fmt.str {sh|git-upload-pack '%s'|sh} path in
    let cmd = Fmt.str "ssh -p %d %s %a" port edn Fmt.(quote string) cmd in
    try
      let ic, oc = Unix.open_process cmd in
      Lwt.return_ok { ic; oc }
    with Unix.Unix_error (err, f, v) -> Lwt.return_error (`Error (err, f, v))

  let read t =
    let tmp = Bytes.create 0x1000 in
    try
      let len = input t.ic tmp 0 0x1000 in
      if len = 0
      then Lwt.return_ok `Eof
      else Lwt.return_ok (`Data (Cstruct.of_bytes tmp ~off:0 ~len))
    with Unix.Unix_error (err, f, v) -> Lwt.return_error (err, f, v)

  let write t cs =
    let str = Cstruct.to_string cs in
    try
      output_string t.oc str ;
      flush t.oc ;
      Lwt.return_ok ()
    with Unix.Unix_error (err, f, v) -> Lwt.return_error (`Error (err, f, v))

  let writev t css =
    let rec go t = function
      | [] -> Lwt.return_ok ()
      | x :: r -> (
          write t x >>= function
          | Ok () -> go t r
          | Error _ as err -> Lwt.return err) in
    go t css

  let close t =
    close_in t.ic ;
    close_out t.oc ;
    Lwt.return_unit
end

module FIFO = struct
  type flow = {
    ic : Lwt_unix.file_descr;
    oc : Lwt_unix.file_descr;
    linger : Bytes.t;
    mutable closed : bool;
  }

  type endpoint = Fpath.t * Fpath.t
  type error = |
  type write_error = [ `Closed ]

  let pp_error : error Fmt.t = fun _ppf -> function _ -> .
  let closed_by_peer = "Closed by peer"
  let pp_write_error ppf = function `Closed -> Fmt.string ppf closed_by_peer
  let io_buffer_size = 65536

  let connect (ic, oc) =
    let open Lwt.Infix in
    Lwt_unix.openfile (Fpath.to_string ic) Unix.[ O_RDONLY ] 0o600 >>= fun ic ->
    Lwt_unix.openfile (Fpath.to_string oc) Unix.[ O_WRONLY ] 0o600 >>= fun oc ->
    Lwt.return_ok
      { ic; oc; linger = Bytes.create io_buffer_size; closed = false }

  let read { ic; linger; closed; _ } =
    if closed
    then Lwt.return_ok `Eof
    else
      Lwt_unix.read ic linger 0 (Bytes.length linger) >>= function
      | 0 -> Lwt.return_ok `Eof
      | len -> Lwt.return_ok (`Data (Cstruct.of_bytes linger ~off:0 ~len))

  let write { oc; closed; _ } cs =
    if closed
    then Lwt.return_error `Closed
    else
      let rec go ({ Cstruct.buffer; off; len } as cs) =
        if len = 0
        then Lwt.return_ok ()
        else
          Lwt_bytes.write oc buffer off len >>= fun len ->
          go (Cstruct.shift cs len) in
      go cs

  let writev t css =
    let rec go = function
      | [] -> Lwt.return_ok ()
      | hd :: tl -> (
          write t hd >>= function
          | Ok () -> go tl
          | Error _ as err -> Lwt.return err) in
    go css

  let close t = Lwt_unix.close t.ic >>= fun () -> Lwt_unix.close t.oc
end

let ssh_edn, ssh_protocol = Mimic.register ~name:"ssh" (module SSH)
let fifo_edn, fifo_protocol = Mimic.register ~name:"fifo" (module FIFO)

module HTTP = struct
  type state =
    | Handshake
    | Get of {
        advertised_refs : string;
        uri : Uri.t;
        headers : (string * string) list;
        ctx : Mimic.ctx;
      }
    | Post of {
        mutable output : string;
        uri : Uri.t;
        headers : (string * string) list;
        ctx : Mimic.ctx;
      }
    | Error

  type flow = state ref
  type error = [ `Msg of string ]
  type write_error = [ `Closed | `Msg of string ]

  let pp_error ppf (`Msg err) = Fmt.string ppf err

  let pp_write_error ppf = function
    | `Closed -> Fmt.string ppf "Connection closed by peer"
    | `Msg err -> Fmt.string ppf err

  let write t cs =
    match t.contents with
    | Handshake | Get _ -> Lwt.return_error (`Msg "Handshake has not been done")
    | Error -> Lwt.return_error (`Msg "Handshake got an error")
    | Post ({ output; _ } as v) ->
        let output = output ^ Cstruct.to_string cs in
        v.output <- output ;
        Lwt.return_ok ()

  let writev t css =
    let rec go = function
      | [] -> Lwt.return_ok ()
      | x :: r -> (
          write t x >>= function
          | Ok () -> go r
          | Error _ as err -> Lwt.return err) in
    go css

  let read t =
    match t.contents with
    | Handshake -> Lwt.return_error (`Msg "Handshake has not been done")
    | Error -> Lwt.return_error (`Msg "Handshake got an error")
    | Get { advertised_refs; uri; headers; ctx } ->
        t.contents <- Post { output = ""; uri; headers; ctx } ;
        Lwt.return_ok (`Data (Cstruct.of_string advertised_refs))
    | Post { output; uri; headers; ctx } -> (
        Git_paf.post ~ctx ~headers uri output >>= function
        | Ok (_resp, contents) ->
            Lwt.return_ok (`Data (Cstruct.of_string contents))
        | Error err ->
            Lwt.return_error (`Msg (Fmt.str "%a" Git_paf.pp_error err)))

  let close _ = Lwt.return_unit

  type endpoint = HTTP_endpoint

  let connect HTTP_endpoint = Lwt.return_ok { contents = Handshake }
end

let unix_ctx_with_ssh () =
  Git_unix.ctx (Happy_eyeballs_lwt.create ()) >|= fun ctx ->
  let open Mimic in
  let k0 scheme user path host port =
    match (scheme, Unix.gethostbyname host) with
    | `SSH, { Unix.h_addr_list; _ } when Array.length h_addr_list > 0 ->
        Lwt.return_some { SSH.user; path; host = h_addr_list.(0); port }
    | _ -> Lwt.return_none in
  ctx
  |> Mimic.fold Smart_git.git_transmission
       Fun.[ req Smart_git.git_scheme ]
       ~k:(function `SSH -> Lwt.return_some `Exec | _ -> Lwt.return_none)
  |> Mimic.fold ssh_edn
       Fun.
         [
           req Smart_git.git_scheme;
           req Smart_git.git_ssh_user;
           req Smart_git.git_path;
           req Smart_git.git_hostname;
           dft Smart_git.git_port 22;
         ]
       ~k:k0

let () = Lwt_preemptive.simple_init ()

let make_temp_fifo mode dir pat =
  let err () =
    R.error_msgf "create temporary fifo %s in %a: too many failing attempts"
      (Fmt.str pat "XXXXXX") Fpath.pp dir in
  let rec loop count =
    if count < 0
    then err ()
    else
      let file =
        let rand = Random.bits () land 0xffffff in
        Fpath.(dir / Fmt.str pat (Fmt.str "%06x" rand)) in
      let sfile = Fpath.to_string file in
      try
        Unix.mkfifo sfile mode ;
        Ok file
      with
      | Unix.Unix_error (Unix.EEXIST, _, _) -> loop (count - 1)
      | Unix.Unix_error (Unix.EINTR, _, _) -> loop count
      | Unix.Unix_error (e, _, _) ->
          R.error_msgf "create temporary fifo %a: %s" Fpath.pp file
            (Unix.error_message e) in
  loop 1000

let git_upload_pack path ic oc =
  let open Bos in
  OS.Dir.with_current path @@ fun () ->
  let tee = Cmd.(v "tee" % Fpath.to_string ic) in
  let cat = Cmd.(v "cat" % Fpath.to_string oc) in
  let git_upload_pack = Cmd.(v "git-upload-pack" % ".") in
  let pipe () =
    let open Rresult in
    OS.Cmd.run_out cat |> OS.Cmd.out_run_in >>= fun cat ->
    OS.Cmd.run_io git_upload_pack cat |> OS.Cmd.out_run_in >>= fun git ->
    OS.Cmd.run_io tee git |> OS.Cmd.to_null in
  match Lwt_unix.fork () with
  | 0 ->
      R.failwith_error_msg (pipe ()) ;
      exit 0
  | pid -> pid

let is_a_git_repository path =
  Logs.debug (fun m ->
      m "%a is a Git repository?" Fpath.pp Fpath.(path / ".git")) ;
  Bos.OS.Dir.exists Fpath.(path / ".git") |> Lwt.return

let pp_scheme ppf = function
  | `Scheme scheme -> Fmt.pf ppf "%s://" scheme
  | `SSH -> Fmt.pf ppf "ssh://"
  | `HTTP -> Fmt.pf ppf "http://"
  | `HTTPS -> Fmt.pf ppf "https://"
  | `Git -> Fmt.pf ppf "git://"

let pp_host ppf = function
  | `Addr addr -> Ipaddr.pp ppf addr
  | `Domain v -> Domain_name.pp ppf v
  | `Name v -> Fmt.string ppf v

let root = Fpath.v "/"
let () = assert (Fpath.is_root root) (* TODO(dinosaure): Windows support. *)

let fetch_local_git_repository edn want path output block_size =
  let tmp = Bos.OS.Dir.default_tmp () in
  make_temp_fifo 0o644 tmp "ic-%s" |> Lwt.return >>? fun ic ->
  make_temp_fifo 0o644 tmp "oc-%s" |> Lwt.return >>? fun oc ->
  git_upload_pack path ic oc () |> Lwt.return >>? fun pid ->
  Git.Mem.Store.v (Fpath.v ".") >|= R.reword_error store_error >>? fun store ->
  OS.File.tmp "pack-%s.pack" |> Lwt.return >>? fun src ->
  OS.File.tmp "pack-%s.pack" |> Lwt.return >>? fun dst ->
  OS.File.tmp "pack-%s.idx" |> Lwt.return >>? fun idx ->
  let create_pack_stream () = stream_of_file dst in
  let create_idx_stream () = stream_of_file idx in
  let _, threads = Lwt_preemptive.get_bounds () in
  let k0 scheme host rest =
    Logs.debug (fun m ->
        m "Try to resolve scheme:%a, host:%s, path:%s." pp_scheme scheme host
          rest) ;
    match scheme with
    | `Scheme "file" -> (
        let path' =
          match Fpath.of_string (host ^ rest) with
          | Ok v when Fpath.is_rooted ~root v -> Ok v
          | Ok x -> Ok Fpath.(root // x)
          | Error _ as err -> err in
        match path' with
        | Ok path' when Fpath.equal path path' -> Lwt.return_some (ic, oc)
        | _ -> Lwt.return_none)
    | `Scheme "relative" -> (
        let cwd = Sys.getcwd () in
        let path' =
          match Fpath.of_string (host ^ rest) with
          | Ok v when Fpath.is_rooted ~root v ->
              let rst = Option.get (Fpath.relativize ~root v) in
              Ok Fpath.(v cwd // rst)
          | Ok rst -> Ok Fpath.(v cwd // rst)
          | Error _ as err -> err in
        match path' with
        | Ok path' when Fpath.equal path path' -> Lwt.return_some (ic, oc)
        | _ -> Lwt.return_none)
    | _ -> Lwt.return_none in
  let ctx =
    let open Mimic in
    let open Smart_git in
    Mimic.empty
    |> Mimic.fold git_transmission
         Fun.[ req git_scheme ]
         ~k:(function
           | `Scheme "file" | `Scheme "relativize" -> Lwt.return_some `Exec
           | _ -> Lwt.return_none)
    |> Mimic.fold fifo_edn
         Fun.[ req git_scheme; req git_hostname; req git_path ]
         ~k:k0 in
  Sync.fetch ~threads ~ctx edn store ~deepen:(`Depth 1)
    (`Some [ (want, want) ])
    ~src ~dst ~idx ~create_pack_stream ~create_idx_stream () ()
  >|= R.reword_error sync_error
  >>? function
  | Some (_, [ (_, hash) ]) ->
      Lwt_unix.waitpid [] pid >>= fun _ ->
      let idx_offset =
        Int64.add
          (Unix.LargeFile.stat (Fpath.to_string dst)).Unix.LargeFile.st_size
          (Int64.of_int (Digestif.SHA1.digest_size + 8)) in
      let idx_remain = Int64.sub block_size (Int64.rem idx_offset block_size) in
      let idx_offset = Int64.add idx_offset idx_remain in
      save ~offset:idx_offset idx dst hash output block_size
  | _ -> Lwt.return_error (R.msgf "%a not found" Git.Reference.pp want)

let fetch edn want date_time output block_size =
  match edn with
  | {
   Smart_git.Endpoint.scheme = `Scheme (("file" | "relativize") as scheme);
   path;
   hostname;
   _;
  } -> (
      let path =
        match scheme with
        | "file" -> (
            match Fpath.of_string (hostname ^ path) with
            | Ok v when Fpath.is_rooted ~root v -> Ok v
            | Ok x -> Ok Fpath.(root // x)
            | Error _ as err -> err)
        | "relativize" -> (
            let cwd = Sys.getcwd () in
            match Fpath.of_string (hostname ^ path) with
            | Ok v when Fpath.is_rooted ~root v ->
                let rst = Option.get (Fpath.relativize ~root v) in
                Ok Fpath.(v cwd // rst)
            | Ok rst -> Ok Fpath.(v cwd // rst)
            | Error _ as err -> err)
        | _ -> assert false in
      path |> R.open_error_msg |> Lwt.return >>? fun path ->
      is_a_git_repository path >>? function
      | true -> fetch_local_git_repository edn want path output block_size
      | false when Sys.file_exists (Fpath.to_string path) ->
          Gitify.gitify ?date_time path output block_size
      | false -> Lwt.return_error (R.msgf "%a does not exists" Fpath.pp path))
  | edn -> (
      unix_ctx_with_ssh () >>= fun ctx ->
      Git.Mem.Store.v (Fpath.v ".") >|= R.reword_error store_error
      >>? fun store ->
      OS.File.tmp "pack-%s.pack" |> Lwt.return >>? fun src ->
      OS.File.tmp "pack-%s.pack" |> Lwt.return >>? fun dst ->
      OS.File.tmp "pack-%s.idx" |> Lwt.return >>? fun idx ->
      let create_pack_stream () = stream_of_file dst in
      let create_idx_stream () = stream_of_file idx in
      let _, threads = Lwt_preemptive.get_bounds () in
      Sync.fetch ~threads ~ctx edn store ~deepen:(`Depth 1)
        (`Some [ (want, want) ])
        ~src ~dst ~idx ~create_pack_stream ~create_idx_stream () ()
      >|= R.reword_error sync_error
      >>? function
      | Some (_, [ (_, hash) ]) ->
          let idx_offset =
            Int64.add
              (Unix.LargeFile.stat (Fpath.to_string dst)).Unix.LargeFile.st_size
              (Int64.of_int (Digestif.SHA1.digest_size + 8)) in
          let idx_remain =
            Int64.sub block_size (Int64.rem idx_offset block_size) in
          let idx_offset = Int64.add idx_offset idx_remain in
          save ~offset:idx_offset idx dst hash output block_size
      | _ -> Lwt.return_error (R.msgf "%a not found" Git.Reference.pp want))

let main _ edn want date_time output block_size =
  match Lwt_main.run (fetch edn want date_time output block_size) with
  | Ok () -> `Ok ()
  | Error (`Msg err) -> `Error (false, Fmt.str "%s." err)
  | Error (`Store err) -> `Error (false, Fmt.str "%a." Store.pp_error err)
  | Error (`Sync err) -> `Error (false, Fmt.str "%a." Sync.pp_error err)

open Cmdliner

let endpoint = Arg.conv (Smart_git.Endpoint.of_string, Smart_git.Endpoint.pp)
let want = Arg.conv (Git.Reference.of_string, Git.Reference.pp)
let output = Arg.conv (Fpath.of_string, Fpath.pp)

let date_time =
  let parser str =
    match Ptime.of_rfc3339 str with
    | Ok (ptime, tz_offset_s, _) -> Ok (ptime, tz_offset_s)
    | Error _ -> R.error_msgf "Invalid date (RFC3339): %S" str in
  Arg.conv
    ( parser,
      fun ppf (ptime, tz_offset_s) -> Ptime.pp_rfc3339 ?tz_offset_s () ppf ptime
    )

let endpoint =
  let doc = "URI leading to repository." in
  Arg.(
    required & pos 0 (some endpoint) None & info [] ~docv:"<repository>" ~doc)

let want =
  let doc = "Reference to pull." in
  Arg.(
    value
    & opt want Git.Reference.master
    & info [ "b"; "branch" ] ~docv:"<reference>" ~doc)

let output =
  let doc = "Disk image." in
  Arg.(required & pos 1 (some output) None & info [] ~docv:"<filename>" ~doc)

let block_size =
  let doc = "Write up to <bytes> bytes." in
  Arg.(value & opt int64 512L & info [ "bs" ] ~docv:"<bytes>" ~doc)

let date_time =
  let doc = "Date & Time (RFC3339)." in
  Arg.(
    value & opt (some date_time) None & info [ "d" ] ~docv:"<date-time>" ~doc)

let setup_logs =
  Term.(const setup_logs $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let command =
  let doc = "Fetch and make an image disk from a Git repository." in
  Cmd.v (Cmd.info "make" ~doc)
    Term.(
      ret
        (const main
        $ setup_logs
        $ endpoint
        $ want
        $ date_time
        $ output
        $ block_size))

let () = Cmd.(exit @@ eval command)
