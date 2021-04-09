type error =
  [ `Invalid_store
  | `Msg of string
  | `Dictionary_expected of Mirage_kv.Key.t
  | `Not_found of Mirage_kv.Key.t
  | `Value_expected of Mirage_kv.Key.t ]

include Mirage_kv.RO with type error := error

val connect : string -> (t, [> error ]) result Lwt.t
