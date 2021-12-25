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

open Lwt.Infix

let src = Logs.Src.create "pack" ~doc:"PACK file"

module Log = (val Logs.src_log src : Logs.LOG)
module SHA1 = Digestif.SHA1

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
  root : SHA1.t;
  buffers : buffers Lwt_pool.t;
}

module Commit = Git.Commit.Make (Git.Hash.Make (SHA1))
module Tree = Git.Tree.Make (Git.Hash.Make (SHA1))

let map (handle, (info : solo5_block_info)) ~pos len =
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

let make name handle (info : solo5_block_info) commit cursor =
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
      ~uid_ln:SHA1.digest_size ~uid_rw:SHA1.of_raw_string find in
  let buffers =
    Lwt_pool.create 4 @@ fun () ->
    let z = Bigstringaf.create De.io_buffer_size in
    let w = De.make_window ~bits:15 in
    let allocate _ = w in
    let w = Carton.Dec.W.make ~sector:info.block_size (handle, info) in
    Lwt.return { z; allocate; w } in
  match load pack commit with
  | Ok (`Commit commit) ->
      let root = Commit.tree commit in
      Lwt.return_ok
        {
          name;
          handle;
          capacity = info.capacity;
          block_size = info.block_size;
          root;
          buffers;
          pack;
        }
  | Ok _ ->
      Lwt.return_error
        (Rresult.R.msgf "Unexpected Git object %a" SHA1.pp commit)
  | Error _ as err -> Lwt.return err

let connect name =
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
          make name handle info commit index
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

let tree_find tree name =
  let res = ref None in
  List.iter
    (fun ({ Git.Tree.name = name'; _ } as entry) ->
      if name = name' then res := Some entry)
    (Git.Tree.to_list tree) ;
  !res

let load t key =
  let rec fold lst hash value =
    match (lst, value) with
    | [], value -> Lwt.return_ok (hash, value)
    | _ :: _, (`Commit _ | `Tag | `Blob _) ->
        Lwt.return_error (`Value_expected key)
    | x :: r, `Tree tree ->
    match tree_find tree x with
    | None -> Lwt.return_error (`Not_found key)
    | Some { Git.Tree.node; _ } -> (
        Lwt_pool.use t.buffers (with_ressources t.pack node) >>= function
        | Ok value -> fold r node value
        | Error _ as err -> Lwt.return err) in
  let lst = Fpath.v (Mirage_kv.Key.to_string key) in
  let lst = Fpath.segs lst in
  Lwt_pool.use t.buffers (with_ressources t.pack t.root) >>= function
  | Ok value -> fold (List.tl lst) t.root value
  | Error _ as err -> Lwt.return err

let exists t key =
  load t key >>= function
  | Ok (_, `Blob _) -> Lwt.return_ok (Some `Value)
  | Ok (_, `Tree _) -> Lwt.return_ok (Some `Dictionary)
  | _ -> Lwt.return_ok None

let get t key =
  load t key >>= function
  | Ok (_, `Blob value) -> Lwt.return_ok (Bigstringaf.to_string value)
  | Ok _ -> Lwt.return_error (`Value_expected key)
  | Error _ as err -> Lwt.return err

let list t key =
  load t key >>= function
  | Ok (_, `Tree tree) ->
      let f acc { Git.Tree.name; perm; _ } =
        match perm with
        | `Everybody | `Normal -> (name, `Value) :: acc
        | `Dir -> (name, `Dictionary) :: acc
        | _ -> acc in
      let lst = List.fold_left f [] (Git.Tree.to_list tree) in
      Lwt.return_ok lst
  | Ok _ -> Lwt.return_error (`Dictionary_expected key)
  | Error _ as err -> Lwt.return err

let digest t key =
  load t key >>= function
  | Ok (hash, _) -> Lwt.return_ok (SHA1.to_raw_string hash)
  | Error _ as err -> Lwt.return err

let last_modified _t _key = Lwt.return_ok (0, 0L)
