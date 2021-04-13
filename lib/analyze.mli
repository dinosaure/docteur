module SHA1 : sig
  include Carton.UID with type t = Digestif.SHA1.t

  include Digestif.S with type t := t and type ctx := ctx
end

module Scheduler : Carton.SCHEDULER with type +'a s = 'a Lwt.t

module Lwt_scheduler : Carton.IO with type +'a t = 'a Lwt.t

module Verify :
    module type of Carton.Dec.Verify (SHA1) (Scheduler) (Lwt_scheduler)

val first_pass :
  read:('fd, Scheduler.t) Carton.Dec.read ->
  block_size:int64 ->
  get_block:
    ('fd -> int64 -> Bigstringaf.t -> int -> int -> (unit, 'error) result) ->
  'fd ->
  ( Verify.status array * Digestif.SHA1.t Carton.Dec.oracle,
    Scheduler.t )
  Carton.io

val map :
  'fd ->
  block_size:int64 ->
  get_block:
    ('fd -> int64 -> Bigstringaf.t -> int -> int -> (unit, 'error) result) ->
  pos:int64 ->
  int ->
  Bigstringaf.t

type 'fd buffers = {
  z : Bigstringaf.t;
  allocate : int -> De.window;
  w : 'fd Carton.Dec.W.t;
}

val unpack :
  'fd ->
  read:('fd, Scheduler.t) Carton.Dec.read ->
  block_size:int64 ->
  get_block:
    ('fd -> int64 -> Bigstringaf.t -> int -> int -> (unit, 'error) result) ->
  SHA1.t ->
  ( 'fd buffers Lwt_pool.t
    * ('fd, SHA1.t) Carton.Dec.t
    * SHA1.t Art.t
    * SHA1.t Art.t,
    [> `Invalid_store | `Msg of string ] )
  result
  Lwt.t

val iter :
  'fd ->
  block_size:int64 ->
  capacity:int64 ->
  get_block:
    ('fd -> int64 -> Bigstringaf.t -> int -> int -> (unit, 'error) result) ->
  SHA1.t ->
  int64 ->
  ( 'fd buffers Lwt_pool.t
    * ('fd, SHA1.t) Carton.Dec.t
    * SHA1.t Art.t
    * SHA1.t Art.t,
    [> `Msg of string ] )
  result
  Lwt.t
