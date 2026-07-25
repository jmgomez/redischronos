import std/[options, unittest]
import chronos
import redischronos

import ./contract/kvcontract

type BrokenKvStore = ref object of KvStore

method get(store: BrokenKvStore,
    key: string): Future[Option[string]] {.async.} =
  return some("wrong")

method set(store: BrokenKvStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  discard

method delete(store: BrokenKvStore, key: string): Future[bool] {.async.} =
  return false

method increment(store: BrokenKvStore, key: string): Future[int64] {.async.} =
  return 0

method close(store: BrokenKvStore): Future[void] {.async.} =
  discard

proc openBrokenKv(): Future[KvStore] {.async, gcsafe.} =
  return BrokenKvStore()

suite "contract harness self-test":
  test "a deliberately broken backend fails every probe":
    check (waitFor probeKvContract(openBrokenKv)) ==
      @["missing get", "round trip", "delete", "increment"]
