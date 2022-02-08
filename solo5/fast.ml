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

exception Unspecified of string

let invalid_arg fmt = Fmt.kstr invalid_arg fmt
let unspecified fmt = Fmt.kstr (fun str -> raise (Unspecified str)) fmt

open Analyze
open Solo5_os.Solo5

type solo5_block_info = { capacity : int64; block_size : int64 }

external solo5_block_acquire : string -> solo5_result * int64 * solo5_block_info
  = "mirage_solo5_block_acquire"

external solo5_block_read :
  int64 -> int64 -> Cstruct.buffer -> int -> int -> solo5_result
  = "mirage_solo5_block_read_3"

let disconnect _id = Lwt.return_unit

let read (handle, info) buf ~off ~len =
  assert (len <= Int64.to_int info.block_size - SHA1.length) ;
  let tmp = Bigstringaf.create (Int64.to_int info.block_size) in
  match solo5_block_read handle 0L tmp 0 (Int64.to_int info.block_size) with
  | SOLO5_R_OK ->
      Bigstringaf.blit_to_bytes tmp ~src_off:(SHA1.length + 8) buf ~dst_off:off
        ~len ;
      Scheduler.inj (Lwt.return len)
  | SOLO5_R_EINVAL -> invalid_arg "Block: read(): Invalid argument"
  | SOLO5_R_EUNSPEC -> unspecified "Block: read(): Unspecified error"
  | SOLO5_R_AGAIN -> assert false

let get_block (handle, _info) pos buf off len =
  match solo5_block_read handle pos buf off len with
  | SOLO5_R_OK -> Ok ()
  | SOLO5_R_AGAIN -> assert false
  | SOLO5_R_EINVAL -> invalid_arg "Block: read(): Invalid argument"
  | SOLO5_R_EUNSPEC -> unspecified "Block: read(): Unspecified error"

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

type t = {
  name : string;
  handle : int64;
  capacity : int64;
  block_size : int64;
  pack : (int64 * solo5_block_info, SHA1.t) Carton.Dec.t;
  buffers : (int64 * solo5_block_info) Analyze.buffers Lwt_pool.t;
  directories : SHA1.t Art.t;
  (* TODO(dinosaure): implements [prefix]. *)
  files : SHA1.t Art.t;
}

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
      | SOLO5_R_OK -> (
          let index = Bigstringaf.get_int64_le commit SHA1.length in
          let commit = Bigstringaf.substring commit ~off:0 ~len:SHA1.length in
          let commit = SHA1.of_raw_string commit in
          let ( >>? ) = Lwt_result.bind in
          match analyze with
          | true ->
              unpack (handle, info) ~read ~block_size:info.block_size ~get_block
                commit
              >>? fun (buffers, pack, directories, files) ->
              Lwt.return_ok
                {
                  name;
                  handle;
                  capacity = info.capacity;
                  block_size = info.block_size;
                  pack;
                  buffers;
                  directories;
                  files;
                }
          | false ->
              iter (handle, info) ~block_size:info.block_size
                ~capacity:info.capacity ~get_block commit index
              >>? fun (buffers, pack, directories, files) ->
              Lwt.return_ok
                {
                  name;
                  handle;
                  capacity = info.capacity;
                  block_size = info.block_size;
                  pack;
                  buffers;
                  directories;
                  files;
                })
      | SOLO5_R_AGAIN -> assert false
      | SOLO5_R_EINVAL ->
          invalid_arg "Block: connect(%s): Invalid argument" name
      | SOLO5_R_EUNSPEC ->
          unspecified "Block: connect(%s): Unspecified error" name)

module Commit = Git.Commit.Make (Git.Hash.Make (SHA1))
module Tree = Git.Tree.Make (Git.Hash.Make (SHA1))

let load ~block_size ~get_block pack uid =
  let open Rresult in
  let map = map ~block_size ~get_block in
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

let with_ressources ~block_size ~get_block pack uid buffers =
  Lwt.catch (fun () ->
      let pack = Carton.Dec.with_z buffers.z pack in
      let pack = Carton.Dec.with_allocate ~allocate:buffers.allocate pack in
      let pack = Carton.Dec.with_w buffers.w pack in
      load ~block_size ~get_block pack uid |> Lwt.return)
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
      Lwt_pool.use t.buffers
        (with_ressources ~block_size:t.block_size ~get_block t.pack hash)
      >>= function
      | Ok (`Blob v) -> Lwt.return_ok (Bigstringaf.to_string v)
      | Ok _ -> Lwt.return_error (`Value_expected key)
      | Error _ as err -> Lwt.return err)

let list t key =
  match Art.find_opt t.directories (Art.key (Mirage_kv.Key.to_string key)) with
  | None -> Lwt.return_error (`Not_found key)
  | Some hash -> (
      let open Lwt.Infix in
      Lwt_pool.use t.buffers
        (with_ressources ~block_size:t.block_size ~get_block t.pack hash)
      >>= function
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
