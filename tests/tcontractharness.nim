import std/[options, strutils, unittest]
import chronos
import redischronos

import ./contract/kvcontract
import ./contract/pubsubcontract

type BrokenKvStore = ref object of KvStore
  brokenClosed: bool

method get(store: BrokenKvStore,
    key: string): Future[Option[string]] {.async.} =
  return some("wrong")

method set(store: BrokenKvStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  discard

method delete(store: BrokenKvStore, key: string): Future[bool] {.async.} =
  return key.endsWith("missing-delete")

method increment(store: BrokenKvStore, key: string): Future[int64] {.async.} =
  return 0

method close(store: BrokenKvStore): Future[void] {.async.} =
  if store.brokenClosed:
    raise newException(BackendClosedError, "broken repeated close")
  store.brokenClosed = true

proc openBrokenKv(): Future[KvStore] {.async, gcsafe.} =
  return BrokenKvStore()

type BrokenPubSub = ref object of PubSub
  brokenClosed: bool
  unsubscribeCalls: int

method subscribe(bus: BrokenPubSub, channel: string,
    handler: MessageHandler): Future[Subscription] {.async.} =
  return Subscription()

method publish(bus: BrokenPubSub, channel,
    payload: string): Future[int64] {.async.} =
  return 99

method unsubscribe(bus: BrokenPubSub,
    subscription: Subscription): Future[void] {.async.} =
  inc bus.unsubscribeCalls
  if bus.unsubscribeCalls > 1:
    raise newException(BackendClosedError, "broken repeated unsubscribe")

method close(bus: BrokenPubSub): Future[void] {.async.} =
  if bus.brokenClosed:
    raise newException(BackendClosedError, "broken repeated close")
  bus.brokenClosed = true

proc openBrokenPubSub(
    options: BackendOptions): Future[PubSub] {.async, gcsafe.} =
  return BrokenPubSub()

suite "contract harness self-test":
  test "a deliberately broken backend fails every probe":
    check (waitFor probeKvContract(openBrokenKv)) ==
      @[
        "missing get",
        "round trip",
        "exists",
        "delete existing",
        "delete missing",
        "increment missing",
        "overwrite",
        "empty value",
        "invalid key",
        "negative ttl",
        "atomic increment",
        "invalid increment",
        "invalid value preservation",
        "overflow increment",
        "overflow value preservation",
        "TTL increment",
        "TTL expiry",
        "increment TTL preservation",
        "idempotent close",
        "closed lifecycle"
      ]

  test "a deliberately broken Pub/Sub backend fails every probe":
    check (waitFor probePubSubContract(openBrokenPubSub)) == @[
      "empty publish count",
      "invalid channel",
      "local publish count",
      "delivery",
      "exact channel routing",
      "ordered delivery",
      "idempotent unsubscribe",
      "unsubscribe",
      "handler failure isolation",
      "handler error observation",
      "handler error redaction",
      "state notification",
      "idempotent close",
      "closed lifecycle"
    ]
