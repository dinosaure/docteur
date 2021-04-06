open Mirage

let docteur ?(analyze= false) disk =
  impl @@ object
       inherit base_configurable
       method ty = kv_ro
       method name = Fmt.str "docteur-%a" Key.pp (Key.abstract disk)
       method module_name = "Docteur"
       method! keys = [ Key.abstract disk ]
       method! packages =
         Key.match_ Key.(value target) @@ function
         | #Key.mode_solo5 -> [ package "docteur" ~sublibs:[ "solo5" ] ]
         | _ -> []
       method! configure info =
         let ctx = Info.context info in
         let name = Option.get (Key.get ctx disk) in
         Hashtbl.add Mirage_impl_block.all_blocks name
           { Mirage_impl_block.filename = name; number = 0 } ;
         Ok ()
       method! connect _ _modname _ =
         Fmt.str
           {ocaml|let ( <.> ) f g = fun x -> f (g x) in
                  let f = Rresult.R.(failwith_error_msg <.> reword_error (msgf "%%a" Docteur.pp_error)) in
                  Lwt.map f (Docteur.connect ~analyze:%b %a)|ocaml}
           analyze
           Key.serialize_call (Key.abstract disk)
     end

let disk =
  let doc = Key.Arg.info ~doc:"Name of the image disk." [ "disk" ] in
  Key.(create "disk" Arg.(required string doc))

let file =
  let doc = Key.Arg.info ~doc:"The file to extract." [ "filename" ] in
  Key.(create "filename" Arg.(required string doc))

let simple = foreign "Unikernel.Make" 
  ~keys:[ Key.abstract file ]
  (console @-> kv_ro @-> job)

let console = default_console

let () = register "simple" [ simple $ console $ docteur disk ]
