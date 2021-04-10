(*
 * Copyright (c) 2011 Anil Madhavapeddy <anil@recoil.org>
 * Copyright (c) 2012 Citrix Systems Inc
 * Copyright (c) 2018 Martin Lucina <martin@lucina.net>
 * Copyright (c) 2021 Romain Calascibetta <romain.calascibetta@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

let src = Logs.Src.create "pack" ~doc:"PACK file"

module Log = (val Logs.src_log src : Logs.LOG)

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

  let detach f =
    let th, wk = Lwt.wait () in
    Lwt.async (fun () ->
        let res = f () in
        Lwt.wakeup_later wk res ;
        Lwt.return_unit) ;
    th
end

exception Unspecified of string

let invalid_arg fmt = Fmt.kstr invalid_arg fmt

let unspecified fmt = Fmt.kstr (fun str -> raise (Unspecified str)) fmt

let failwith fmt = Fmt.kstr failwith fmt

open OS.Solo5

type solo5_block_info = { capacity : int64; block_size : int64 }

external solo5_block_acquire : string -> solo5_result * int64 * solo5_block_info
  = "mirage_solo5_block_acquire"

external solo5_block_read :
  int64 -> int64 -> Cstruct.buffer -> int -> int -> solo5_result
  = "mirage_solo5_block_read_3"

let disconnect _id = Lwt.return_unit

module Scheduler = Carton.Make (Lwt)
module Verify = Carton.Dec.Verify (SHA1) (Scheduler) (Lwt_scheduler)
module First_pass = Carton.Dec.Fp (SHA1)
open Scheduler

let scheduler =
  let open Lwt in
  let open Scheduler in
  {
    Carton.bind = (fun x f -> inj (bind (prj x) (fun x -> prj (f x))));
    return = (fun x -> inj (return x));
  }

let read (handle, info) buf ~off ~len =
  assert (len <= Int64.to_int info.block_size - SHA1.length) ;
  let tmp = Bigstringaf.create (Int64.to_int info.block_size) in
  match solo5_block_read handle 0L tmp 0 (Int64.to_int info.block_size) with
  | SOLO5_R_OK ->
      Bigstringaf.blit_to_bytes tmp ~src_off:(SHA1.length + 8) buf ~dst_off:off
        ~len ;
      scheduler.return len
  | SOLO5_R_EINVAL -> invalid_arg "Block: read(): Invalid argument"
  | SOLO5_R_EUNSPEC -> unspecified "Block: read(): Unspecified error"
  | SOLO5_R_AGAIN -> assert false

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

let first_pass (handle, info) =
  let ( >>= ) = scheduler.bind in
  let return = scheduler.return in

  let oc = De.bigstring_create De.io_buffer_size in
  let zw = De.make_window ~bits:15 in
  let tp = ref (Bigstringaf.create (Int64.to_int info.block_size)) in
  let allocate _ = zw in
  First_pass.check_header scheduler read (handle, info) >>= fun (max, _, _) ->
  let decoder = First_pass.decoder ~o:oc ~allocate `Manual in
  let children = Hashtbl.create 0x100 in
  let where = Hashtbl.create 0x100 in
  let weight = Hashtbl.create 0x100 in
  let length = Hashtbl.create 0x100 in
  let carbon = Hashtbl.create 0x100 in
  let matrix = Array.make max Verify.unresolved_node in
  let sector = ref 1 in

  let rec go decoder =
    match First_pass.decode decoder with
    | `Await decoder -> (
        Log.debug (fun m -> m "`Await.") ;
        let offset = Int64.mul (Int64.of_int !sector) info.block_size in
        match
          solo5_block_read handle offset !tp 0 (Int64.to_int info.block_size)
        with
        | SOLO5_R_OK ->
            Log.debug (fun m ->
                m "@[<hov>%a@]"
                  (Hxd_string.pp Hxd.default)
                  (Bigstringaf.to_string !tp)) ;
            incr sector ;
            go (First_pass.src decoder !tp 0 (Int64.to_int info.block_size))
        | _ -> failwith "Block: analyze(): Cannot read ~sector:%d" !sector)
    | `Peek decoder -> (
        let offset = Int64.mul (Int64.of_int !sector) info.block_size in
        let keep = First_pass.src_rem decoder in
        Log.debug (fun m -> m "`Peek (keep %d byte(s))." keep) ;
        let tp' = Bigstringaf.create (keep + Int64.to_int info.block_size) in
        Bigstringaf.blit !tp ~src_off:0 tp' ~dst_off:0 ~len:keep ;
        match
          solo5_block_read handle offset tp' keep (Int64.to_int info.block_size)
        with
        | SOLO5_R_OK ->
            Log.debug (fun m ->
                m "@[<hov>%a@]"
                  (Hxd_string.pp Hxd.default)
                  (Bigstringaf.to_string !tp)) ;
            incr sector ;
            tp := tp' ;
            go
              (First_pass.src decoder tp' 0
                 (keep + Int64.to_int info.block_size))
        | _ -> failwith "Block: analyze(): Cannot read ~sector:%d" !sector)
    | `Entry ({ First_pass.kind = Base _; offset; size; consumed; _ }, decoder)
      ->
        let offset = Int64.add offset (Int64.of_int (SHA1.length + 8)) in
        let n = First_pass.count decoder - 1 in
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
            _;
          },
          decoder ) ->
        let offset = Int64.add offset (Int64.of_int (SHA1.length + 8)) in
        let n = First_pass.count decoder - 1 in
        replace weight Int64.(sub offset (Int64.of_int s)) source ;
        replace weight offset target ;
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
            _;
          },
          decoder ) ->
        let offset = Int64.add offset (Int64.of_int (SHA1.length + 8)) in
        let n = First_pass.count decoder - 1 in
        replace weight offset (Stdlib.max target source) ;
        Hashtbl.add length offset size ;
        Hashtbl.add carbon offset consumed ;
        Hashtbl.add where offset n ;
        (try
           let vs = Hashtbl.find children (`Ref ptr) in
           Hashtbl.replace children (`Ref ptr) (offset :: vs)
         with _ -> Hashtbl.add children (`Ref ptr) [ offset ]) ;
        go decoder
    | `End _hash ->
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
        return (matrix, oracle)
    | `Malformed err -> failwith "Block: analyze(): %s" err in
  match solo5_block_read handle 0L !tp 0 (Int64.to_int info.block_size) with
  | SOLO5_R_OK ->
      let decoder =
        First_pass.src decoder !tp (SHA1.length + 8)
          (Int64.to_int info.block_size - SHA1.length) in
      go decoder
  | _ -> failwith "Block: analyze(): Cannot read ~sector:%d" 0

let map (handle, info) ~pos len =
  Log.debug (fun m ->
      m "map ~pos:%Ld ~len:%d (block_size: %Ld)." pos len info.block_size) ;
  assert (len <= Int64.to_int info.block_size) ;
  assert (Int64.logand pos (Int64.pred info.block_size) = 0L) ;
  let len = Int64.to_int info.block_size in
  let res = Bigstringaf.create len in
  match solo5_block_read handle pos res 0 len with
  | SOLO5_R_OK ->
      Log.debug (fun m ->
          m "mmap: @[<hov>%a@]"
            (Hxd_string.pp Hxd.default)
            (Bigstringaf.to_string res)) ;
      res
  | SOLO5_R_AGAIN -> assert false
  | SOLO5_R_EINVAL -> invalid_arg "Block: read(): Invalid argument"
  | SOLO5_R_EUNSPEC -> unspecified "Block: read(): Unspecified error"

type buffers = {
  z : Bigstringaf.t;
  allocate : int -> De.window;
  w : (int64 * solo5_block_info) Carton.Dec.W.t;
}

type t = {
  name : string;
  handle : int64;
  capacity : int64;
  block_size : int64;
  pack : (int64 * solo5_block_info, SHA1.t) Carton.Dec.t;
  buffers : buffers Lwt_pool.t;
  directories : SHA1.t Art.t;
  (* TODO(dinosaure): implements [prefix]. *)
  files : SHA1.t Art.t;
}

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

let light pack uid =
  let open Rresult in
  let path = Carton.Dec.path_of_uid ~map pack uid in
  match Carton.Dec.kind_of_path path with
  | `C -> R.ok `Blob
  | `D -> R.ok `Tag
  | `A | `B -> (
      let cursor = List.hd (Carton.Dec.path_to_list path) in
      let weight =
        Carton.Dec.weight_of_offset ~map pack ~weight:Carton.Dec.null cursor
      in
      let raw = Carton.Dec.make_raw ~weight in
      let v = Carton.Dec.of_offset_with_path ~map pack ~path raw ~cursor in
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
      | _ -> assert false)
(* XXX(dinosaure): safe. *)

let rec fold pack directories files path hash =
  let open Rresult in
  light pack hash >>= function
  | `Tree tree ->
      let f a { Git.Tree.name; node; perm } =
        match (a, perm) with
        | (Error _ as err), _ -> err
        | Ok _, `Dir ->
            let path = Fpath.(path / name) in
            Art.insert directories (Art.key (Fpath.to_string path)) node ;
            fold pack directories files Fpath.(path / name) node
        | Ok _, (`Everybody | `Normal) ->
            let path = Fpath.(path / name) in
            Art.insert files (Art.key (Fpath.to_string path)) node ;
            R.ok ()
        | (Ok _ as v), _ -> v in
      List.fold_left f (R.ok ()) (Git.Tree.to_list tree)
  | `Commit commit -> fold pack directories files path (Commit.tree commit)
  | `Blob | `Tag -> R.ok ()

let unpack name handle info commit =
  let open Lwt.Infix in
  first_pass (handle, info) |> prj >>= fun (matrix, oracle) ->
  let z = De.bigstring_create De.io_buffer_size in
  let allocate bits = De.make_window ~bits in
  let never _ = assert false in
  let pack =
    Carton.Dec.make ~sector:info.block_size (handle, info) ~allocate ~z
      ~uid_ln:SHA1.length ~uid_rw:SHA1.of_raw_string never in
  Verify.verify ~threads:1 pack ~map ~oracle ~verbose:ignore ~matrix
  >>= fun () ->
  match Array.for_all Verify.is_resolved matrix with
  | false -> Lwt.return_error `Invalid_store
  | true -> (
      let index = Hashtbl.create (Array.length matrix) in
      let iter v =
        let offset = Verify.offset_of_status v in
        let hash = Verify.uid_of_status v in
        Hashtbl.add index hash offset in
      Array.iter iter matrix ;
      let pack =
        Carton.Dec.make ~sector:info.block_size (handle, info) ~allocate ~z
          ~uid_ln:SHA1.length ~uid_rw:SHA1.of_raw_string (Hashtbl.find index)
      in
      let directories = Art.make () in
      let files = Art.make () in
      match fold pack directories files (Fpath.v "/") commit with
      | Ok () ->
          let buffers =
            Lwt_pool.create 4 @@ fun () ->
            let z = Bigstringaf.create De.io_buffer_size in
            let w = De.make_window ~bits:15 in
            let allocate _ = w in
            let w = Carton.Dec.W.make ~sector:info.block_size (handle, info) in
            Lwt.return { z; allocate; w } in
          Lwt.return_ok
            {
              name;
              handle;
              capacity = info.capacity;
              block_size = info.block_size;
              buffers;
              pack;
              directories;
              files;
            }
      | Error _ as err -> Lwt.return err)

let rec split ~block_size index off acc =
  if off = Bigstringaf.length index
  then List.rev acc
  else
    let block = Bigstringaf.sub index ~off ~len:(Int64.to_int block_size) in
    split ~block_size index (off + Int64.to_int block_size) (block :: acc)

let read_one_block handle offset ~off ~len buffer =
  match solo5_block_read handle offset buffer off len with
  | SOLO5_R_OK -> ()
  | SOLO5_R_AGAIN -> assert false
  | SOLO5_R_EINVAL ->
      invalid_arg "Block: read(%Lx:%d): Invalid argument" offset len
  | SOLO5_R_EUNSPEC ->
      unspecified "Block: read(%Lx:%d): Unspecified error" offset len

let read handle offset bs =
  let rec go offset = function
    | [] -> ()
    | x :: r ->
        read_one_block handle offset ~off:0 ~len:(Bigstringaf.length x) x ;
        go (Int64.add offset (Int64.of_int (Bigstringaf.length x))) r in
  go offset bs

let iter name handle (info : solo5_block_info) commit cursor =
  let index =
    Bigstringaf.create (Int64.to_int (Int64.sub info.capacity cursor)) in
  let blocks = split ~block_size:info.block_size index 0 [] in
  read handle cursor blocks ;
  let index =
    Carton.Dec.Idx.make index ~uid_ln:SHA1.digest_size
      ~uid_rw:SHA1.to_raw_string ~uid_wr:SHA1.of_raw_string in
  let z = Bigstringaf.create De.io_buffer_size in
  let zw = De.make_window ~bits:15 in
  let allocate _ = zw in
  let find uid =
    match Carton.Dec.Idx.find index uid with
    | Some (_, offset) -> Int64.add (Int64.of_int (SHA1.digest_size + 8)) offset
    | None -> failwith "%a does not exist" SHA1.pp uid in
  let pack =
    Carton.Dec.make ~sector:info.block_size (handle, info) ~allocate ~z
      ~uid_ln:SHA1.length ~uid_rw:SHA1.of_raw_string find in
  let directories = Art.make () in
  let files = Art.make () in
  match fold pack directories files (Fpath.v "/") commit with
  | Ok () ->
      let buffers =
        Lwt_pool.create 4 @@ fun () ->
        let z = Bigstringaf.create De.io_buffer_size in
        let w = De.make_window ~bits:15 in
        let allocate _ = w in
        let w = Carton.Dec.W.make ~sector:info.block_size (handle, info) in
        Lwt.return { z; allocate; w } in
      Lwt.return_ok
        {
          name;
          handle;
          capacity = info.capacity;
          block_size = info.block_size;
          buffers;
          pack;
          directories;
          files;
        }
  | Error _ as err -> Lwt.return err

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

let connect ?(analyze = false) name =
  match solo5_block_acquire name with
  | SOLO5_R_AGAIN, _, _ ->
      assert false (* not returned by solo5_block_acquire *)
  | SOLO5_R_EINVAL, _, _ ->
      invalid_arg "Block: connect(%s): Invalid argument" name
  | SOLO5_R_EUNSPEC, _, _ ->
      unspecified "Block: connect(%s): Unspecified error" name
  | SOLO5_R_OK, handle, info -> (
      let commit = Bigstringaf.create (Int64.to_int info.block_size) in
      match
        solo5_block_read handle 0L commit 0 (Int64.to_int info.block_size)
      with
      | SOLO5_R_OK ->
          let index = Bigstringaf.get_int64_le commit SHA1.digest_size in
          let commit =
            Bigstringaf.substring commit ~off:0 ~len:SHA1.digest_size in
          let commit = SHA1.of_raw_string commit in
          if analyze
          then unpack name handle info commit
          else iter name handle info commit index
      | SOLO5_R_AGAIN -> assert false
      | SOLO5_R_EINVAL ->
          invalid_arg "Block: connect(%s): Invalid argument" name
      | SOLO5_R_EUNSPEC ->
          unspecified "Block: connect(%s): Unspecified error" name)

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
