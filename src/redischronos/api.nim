from std/options import Option
import chronos

import ./errors
import ./options

export options
export errors

type
  BackendKind* = enum
    bkMemory, bkRedis

  ConnectionState* = enum
    csConnecting, csConnected, csDisconnected, csClosed

  MessageHandler* =
    proc(channel, payload: string): Future[void] {.gcsafe.}

  StateHandler* =
    proc(state: ConnectionState): Future[void] {.gcsafe.}

  KvStore* = ref object of RootObj
    closed: bool

  PubSub* = ref object of RootObj
    closed: bool
    stateHandler: StateHandler

  Subscription* = ref object of RootObj

method get*(store: KvStore, key: string): Future[Option[string]] {.base,
    async.} =
  raise newException(BackendClosedError, "key/value backend is unavailable")

method set*(store: KvStore, key, value: string, ttlSeconds = 0): Future[void] {.
    base, async.} =
  raise newException(BackendClosedError, "key/value backend is unavailable")

method delete*(store: KvStore, key: string): Future[bool] {.base, async.} =
  raise newException(BackendClosedError, "key/value backend is unavailable")

method exists*(store: KvStore, key: string): Future[bool] {.base, async.} =
  raise newException(BackendClosedError, "key/value backend is unavailable")

method increment*(store: KvStore, key: string): Future[int64] {.base, async.} =
  raise newException(BackendClosedError, "key/value backend is unavailable")

method close*(store: KvStore): Future[void] {.base, async.} =
  store.closed = true

method subscribe*(bus: PubSub, channel: string,
    handler: MessageHandler): Future[Subscription] {.base, async.} =
  raise newException(BackendClosedError, "pub/sub backend is unavailable")

method unsubscribe*(bus: PubSub,
    subscription: Subscription): Future[void] {.base, async.} =
  if bus.closed:
    raise newException(BackendClosedError, "pub/sub backend is closed")

method publish*(bus: PubSub, channel, payload: string): Future[int64] {.base,
    async.} =
  raise newException(BackendClosedError, "pub/sub backend is unavailable")

method onStateChange*(bus: PubSub, handler: StateHandler) {.base, gcsafe.} =
  bus.stateHandler = handler

method close*(bus: PubSub): Future[void] {.base, async.} =
  bus.closed = true
