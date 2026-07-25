import std/[options, unittest]
import chronos
import redischronos

import ./contract/kvcontract
import ./contract/pubsubcontract

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

type BrokenPubSub = ref object of PubSub

method subscribe(bus: BrokenPubSub, channel: string,
    handler: MessageHandler): Future[Subscription] {.async.} =
  return Subscription()

method publish(bus: BrokenPubSub, channel,
    payload: string): Future[int64] {.async.} =
  return 99

method unsubscribe(bus: BrokenPubSub,
    subscription: Subscription): Future[void] {.async.} =
  discard

method close(bus: BrokenPubSub): Future[void] {.async.} =
  discard

proc openBrokenPubSub(
    options: BackendOptions): Future[PubSub] {.async, gcsafe.} =
  return BrokenPubSub()

suite "contract harness self-test":
  test "a deliberately broken backend fails every probe":
    check (waitFor probeKvContract(openBrokenKv)) ==
      @[
        "missing get", "round trip", "delete", "increment", "exists",
        "overwrite", "invalid key", "negative ttl", "closed lifecycle"
      ]

  test "a deliberately broken Pub/Sub backend fails every probe":
    check (waitFor probePubSubContract(openBrokenPubSub)) == @[
      "empty publish count",
      "invalid channel",
      "local publish count",
      "delivery",
      "unsubscribe",
      "state notification",
      "closed lifecycle"
    ]
