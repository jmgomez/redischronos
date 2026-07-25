import std/options
import chronos

import ./api
import ./errors
import ./options as backendoptions
import ./redisconnection
import ./redisurl
import ./resp2

type RedisKvStore* = ref object of KvStore
  connection: RedisConnection
  isClosed: bool
  closeTask: Future[void]

proc newRedisKvStore*(url: string,
    options = defaultBackendOptions()): RedisKvStore =
  RedisKvStore(
    connection: newRedisConnection(parseRedisUrl(url), options)
  )

proc requireOpen(store: RedisKvStore) =
  if store.isClosed:
    raise newException(BackendClosedError, "key/value backend is closed")

proc requireKey(key: string) =
  if key.len == 0:
    raise newException(InvalidArgumentError, "key must not be empty")

method get*(store: RedisKvStore,
    key: string): Future[Option[string]] {.async.} =
  store.requireOpen()
  requireKey(key)
  let reply = await store.connection.execute(["GET", key])
  case reply.kind
  of rkNilBulkString:
    return none(string)
  of rkBulkString:
    return some(reply.text)
  else:
    raise newException(ProtocolError, "unexpected Redis GET reply")

method set*(store: RedisKvStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  store.requireOpen()
  requireKey(key)
  if ttlSeconds < 0:
    raise newException(InvalidArgumentError, "TTL must not be negative")
  let reply =
    if ttlSeconds == 0:
      await store.connection.execute(["SET", key, value])
    else:
      await store.connection.execute(["SET", key, value, "EX", $ttlSeconds])
  if reply.kind != rkSimpleString or reply.text != "OK":
    raise newException(ProtocolError, "unexpected Redis SET reply")

method delete*(store: RedisKvStore, key: string): Future[bool] {.async.} =
  store.requireOpen()
  requireKey(key)
  let reply = await store.connection.execute(["DEL", key])
  if reply.kind != rkInteger:
    raise newException(ProtocolError, "unexpected Redis DEL reply")
  return reply.integer != 0

method exists*(store: RedisKvStore, key: string): Future[bool] {.async.} =
  store.requireOpen()
  requireKey(key)
  let reply = await store.connection.execute(["EXISTS", key])
  if reply.kind != rkInteger:
    raise newException(ProtocolError, "unexpected Redis EXISTS reply")
  return reply.integer != 0

method increment*(store: RedisKvStore,
    key: string): Future[int64] {.async.} =
  store.requireOpen()
  requireKey(key)
  let reply = await store.connection.execute(["INCR", key])
  if reply.kind != rkInteger:
    raise newException(ProtocolError, "unexpected Redis INCR reply")
  return reply.integer

proc closeOwned(store: RedisKvStore) {.async.} =
  await store.connection.close()

proc joinClose(store: RedisKvStore): Future[void] {.async.} =
  if store.closeTask == nil:
    store.isClosed = true
    store.closeTask = store.closeOwned()
  await store.closeTask.noCancel()

method close*(store: RedisKvStore): Future[void] =
  store.joinClose()
