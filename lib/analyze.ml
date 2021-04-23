let src = Logs.Src.create "docteur.analyze"

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

let failwith fmt = Fmt.kstr failwith fmt

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

let first_pass ~read ~block_size ~get_block fd =
  Log.debug (fun m -> m "Start to analyze the given PACK file.") ;
  let ( >>= ) = scheduler.bind in
  let return = scheduler.return in

  let oc = De.bigstring_create De.io_buffer_size in
  let zw = De.make_window ~bits:15 in
  let tp = ref (Bigstringaf.create (Int64.to_int block_size)) in
  let allocate _ = zw in
  First_pass.check_header scheduler read fd >>= fun (max, _, _) ->
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
        Log.debug (fun m -> m "`Await") ;
        let offset = Int64.mul (Int64.of_int !sector) block_size in
        match get_block fd offset !tp 0 (Int64.to_int block_size) with
        | Ok () ->
            incr sector ;
            go (First_pass.src decoder !tp 0 (Int64.to_int block_size))
        | _ -> failwith "Block: analyze(): Cannot read ~sector:%d" !sector)
    | `Peek decoder -> (
        Log.debug (fun m -> m "`Peek") ;
        let offset = Int64.mul (Int64.of_int !sector) block_size in
        let keep = First_pass.src_rem decoder in
        let tp' = Bigstringaf.create (keep + Int64.to_int block_size) in
        Bigstringaf.blit !tp ~src_off:0 tp' ~dst_off:0 ~len:keep ;
        match get_block fd offset tp' keep (Int64.to_int block_size) with
        | Ok () ->
            incr sector ;
            tp := tp' ;
            go (First_pass.src decoder tp' 0 (keep + Int64.to_int block_size))
        | _ -> failwith "Block: analyze(): Cannot read ~sector:%d" !sector)
    | `Entry ({ First_pass.kind = Base _; offset; size; consumed; _ }, decoder)
      ->
        Log.debug (fun m -> m "[+] base object") ;
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
        Log.debug (fun m -> m "[+] ofs object") ;
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
        Log.debug (fun m -> m "[+] ref object") ;
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
  match get_block fd 0L !tp 0 (Int64.to_int block_size) with
  | Ok () ->
      let decoder =
        First_pass.src decoder !tp (SHA1.length + 8)
          (Int64.to_int block_size - SHA1.length - 8) in
      go decoder
  | _ -> failwith "Block: analyze(): Cannot read ~sector:%d" 0

let map fd ~block_size ~get_block ~pos len =
  assert (len <= Int64.to_int block_size) ;
  assert (Int64.logand pos (Int64.pred block_size) = 0L) ;
  let len = Int64.to_int block_size in
  let res = Bigstringaf.create len in
  match get_block fd pos res 0 len with Ok () -> res | Error _ -> assert false

type a_and_b = [ `A | `B ]

module Commit = Git.Commit.Make (Git.Hash.Make (SHA1))
module Tree = Git.Tree.Make (Git.Hash.Make (SHA1))

let load ~block_size ~get_block pack uid =
  let open Rresult in
  let map = map ~block_size ~get_block in
  let path = Carton.Dec.path_of_uid ~map pack uid in
  match Carton.Dec.kind_of_path path with
  | `C -> R.ok `Blob
  | `D -> R.ok `Tag
  | #a_and_b as kind -> (
      let cursor = List.hd (Carton.Dec.path_to_list path) in
      let weight =
        Carton.Dec.weight_of_offset ~map pack ~weight:Carton.Dec.null cursor
      in
      let raw = Carton.Dec.make_raw ~weight in
      let v = Carton.Dec.of_offset_with_path ~map pack ~path raw ~cursor in
      match kind with
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
          >>| fun v -> `Tree v)

let rec fold ~block_size ~get_block pack directories files path hash =
  let open Rresult in
  load ~block_size ~get_block pack hash >>= function
  | `Tree tree ->
      let f a { Git.Tree.name; node; perm } =
        match (a, perm) with
        | (Error _ as err), _ -> err
        | Ok _, `Dir ->
            let path = Fpath.(path / name) in
            Art.insert directories (Art.key (Fpath.to_string path)) node ;
            fold ~block_size ~get_block pack directories files path node
        | Ok _, (`Everybody | `Normal) ->
            let path = Fpath.(path / name) in
            Art.insert files (Art.key (Fpath.to_string path)) node ;
            R.ok ()
        | (Ok _ as v), _ -> v in
      List.fold_left f (R.ok ()) (Git.Tree.to_list tree)
  | `Commit commit ->
      fold ~block_size ~get_block pack directories files path
        (Commit.tree commit)
  | `Blob | `Tag -> R.ok ()

type 'fd buffers = {
  z : Bigstringaf.t;
  allocate : int -> De.window;
  w : 'fd Carton.Dec.W.t;
}

let unpack fd ~read ~block_size ~get_block commit =
  let open Lwt.Infix in
  let map = map ~block_size ~get_block in
  Log.debug (fun m -> m "Start to analyze the PACK file.") ;
  first_pass ~read ~block_size ~get_block fd |> prj >>= fun (matrix, oracle) ->
  let z = De.bigstring_create De.io_buffer_size in
  let allocate bits = De.make_window ~bits in
  let never _ = assert false in
  let pack =
    Carton.Dec.make ~sector:block_size fd ~allocate ~z ~uid_ln:SHA1.length
      ~uid_rw:SHA1.of_raw_string never in
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
        Carton.Dec.make ~sector:block_size fd ~allocate ~z ~uid_ln:SHA1.length
          ~uid_rw:SHA1.of_raw_string (Hashtbl.find index) in
      let directories = Art.make () in
      let files = Art.make () in
      match
        fold ~block_size ~get_block pack directories files (Fpath.v "/") commit
      with
      | Ok () ->
          let buffers =
            Lwt_pool.create 4 @@ fun () ->
            let z = Bigstringaf.create De.io_buffer_size in
            let w = De.make_window ~bits:15 in
            let allocate _ = w in
            let w = Carton.Dec.W.make ~sector:block_size fd in
            Lwt.return { z; allocate; w } in
          Lwt.return_ok (buffers, pack, directories, files)
      | Error _ as err -> Lwt.return err)

let read fd ~get_block offset bs =
  let rec go offset = function
    | [] -> ()
    | x :: r ->
    match get_block fd offset x 0 (Bigstringaf.length x) with
    | Ok () -> go (Int64.add offset (Int64.of_int (Bigstringaf.length x))) r
    | Error _ -> failwith "Block: iter(): Cannot read at %Ld" offset in
  go offset bs

let rec split ~block_size index off acc =
  if off = Bigstringaf.length index
  then List.rev acc
  else
    let block = Bigstringaf.sub index ~off ~len:(Int64.to_int block_size) in
    split ~block_size index (off + Int64.to_int block_size) (block :: acc)

let iter fd ~block_size ~capacity ~get_block commit cursor =
  let index = Bigstringaf.create (Int64.to_int (Int64.sub capacity cursor)) in
  let blocks = split ~block_size index 0 [] in
  read fd ~get_block cursor blocks ;
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
    Carton.Dec.make ~sector:block_size fd ~allocate ~z ~uid_ln:SHA1.length
      ~uid_rw:SHA1.of_raw_string find in
  let directories = Art.make () in
  let files = Art.make () in
  match
    fold ~block_size ~get_block pack directories files (Fpath.v "/") commit
  with
  | Ok () ->
      let buffers =
        Lwt_pool.create 4 @@ fun () ->
        let z = Bigstringaf.create De.io_buffer_size in
        let w = De.make_window ~bits:15 in
        let allocate _ = w in
        let w = Carton.Dec.W.make ~sector:block_size fd in
        Lwt.return { z; allocate; w } in
      Lwt.return_ok (buffers, pack, directories, files)
  | Error _ as err -> Lwt.return err
