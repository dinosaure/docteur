open Lwt.Infix

module Make (Console : Mirage_console.S) (Store : Mirage_kv.RO) = struct
  module Key = Mirage_kv.Key

  let log console fmt = Fmt.kstr (Console.log console) fmt

  let start console store =
    Store.get store (Key.v (Key_gen.filename ())) >>= function
    | Ok str -> log console "%S\n%!" str
    | Error err -> log console "ER: %a.\n%!" Store.pp_error err
end
