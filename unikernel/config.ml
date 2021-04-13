open Mirage

let pp_mode ppf = function
  | `Light -> Fmt.string ppf "Light"
  | `Fast -> Fmt.string ppf "Fast"

let docteur_unix ?(mode= `Fast) disk =
  impl @@ object
       inherit base_configurable
       method ty = kv_ro
       method name = Fmt.str "docteur-unix-%a" Key.pp (Key.abstract disk)
       method module_name = Fmt.str "Docteur_unix.%a" pp_mode mode
       method! keys = [ Key.abstract disk ]
       method! packages = Key.pure [ package "docteur" ~sublibs:[ "unix" ] ]
       method! configure info =
         let ctx = Info.context info in
         let name = Option.get (Key.get ctx disk) in
         Hashtbl.add Mirage_impl_block.all_blocks name
           { Mirage_impl_block.filename = name; number = 0 } ;
         Ok ()
       method! connect _info modname _ =
         Fmt.str
           {ocaml|let ( <.> ) f g = fun x -> f (g x) in
                  let f = Rresult.R.(failwith_error_msg <.> reword_error (msgf "%%a" %s.pp_error)) in
                  Lwt.map f (%s.connect ~analyze:true %a)|ocaml}
           modname modname Key.serialize_call (Key.abstract disk)
     end

let docteur_solo5 ?(mode= `Fast) disk =
  impl @@ object
       inherit base_configurable
       method ty = kv_ro
       method name = Fmt.str "docteur-solo5-%a" Key.pp (Key.abstract disk)
       method module_name = Fmt.str "Docteur_solo5.%a" pp_mode mode
       method! keys = [ Key.abstract disk ]
       method! packages = Key.pure [ package "docteur" ~sublibs:[ "solo5" ] ]
       method! configure info =
         let ctx = Info.context info in
         let name = Option.get (Key.get ctx disk) in
         Hashtbl.add Mirage_impl_block.all_blocks name
           { Mirage_impl_block.filename = name; number = 0 } ;
         Ok ()
       method! connect _info modname _ =
         Fmt.str
           {ocaml|let ( <.> ) f g = fun x -> f (g x) in
                  let f = Rresult.R.(failwith_error_msg <.> reword_error (msgf "%%a" %s.pp_error)) in
                  Lwt.map f (%s.connect %a)|ocaml}
           modname modname Key.serialize_call (Key.abstract disk)
     end

let docteur ?mode disk =
  match_impl Key.(value target)
  [ (`Unix,   docteur_unix  ?mode disk)
  ; (`MacOSX, docteur_unix  ?mode disk)
  ; (`Hvt,    docteur_solo5 ?mode disk)
  ; (`Spt,    docteur_solo5 ?mode disk)
  ; (`Virtio, docteur_solo5 ?mode disk)
  ; (`Muen,   docteur_solo5 ?mode disk)
  ; (`Genode, docteur_solo5 ?mode disk) ]
  ~default:(docteur_unix ?mode disk)

let disk =
  let doc = Key.Arg.info ~doc:"Name of the image disk." [ "disk" ] in
  Key.(create "disk" Arg.(required ~stage:`Configure string doc))

let file =
  let doc = Key.Arg.info ~doc:"The file to extract." [ "filename" ] in
  Key.(create "filename" Arg.(required ~stage:`Both string doc))

let simple = foreign "Unikernel.Make" 
  ~keys:[ Key.abstract file ]
  (console @-> kv_ro @-> job)

let console = default_console

let () = register "simple" [ simple $ console $ docteur disk ]
