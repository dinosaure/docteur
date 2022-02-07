open Mirage

let remote =
  let doc = Key.Arg.info ~doc:"The remote Git repository." [ "r"; "remote" ] in
  Key.(create "remote" Arg.(required ~stage:`Configure string doc))

let filename = 
  let doc = Key.Arg.info ~doc:"The filename to print out." [ "filename" ] in
  Key.(create "filename" Arg.(required string doc))

let simple = foreign "Unikernel.Make" 
  ~keys:[ Key.v filename ]
  (console @-> kv_ro @-> job)

let console = default_console

let () = register "simple" [ simple $ console $ docteur remote ]
