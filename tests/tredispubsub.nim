import std/[os, unittest]
import chronos
import redischronos
import redischronos/redispubsub

import ./contract/pubsubcontract

when defined(redisIntegration):
  doAssert getEnv("REDIS_TEST_URL").len > 0,
    "REDIS_TEST_URL is mandatory for Redis integration tests"

  proc openRedisPubSub(options: BackendOptions): Future[PubSub] {.gcsafe.} =
    openPubSub(getEnv("REDIS_TEST_URL"), options)

  pubSubContractSuite("Redis", openRedisPubSub)

  suite "Redis Pub/Sub mappings":
    test "multiple local handlers share one server subscription":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        var first, second: int
        let firstHandler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            inc first
        let secondHandler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            inc second
        let firstSubscription =
          await bus.subscribe("redischronos:shared", firstHandler)
        let secondSubscription =
          await bus.subscribe("redischronos:shared", secondHandler)
        check (await bus.publish("redischronos:shared", "one")) == 1
        await sleepAsync(20.milliseconds)
        check first == 1
        check second == 1
        await bus.unsubscribe(firstSubscription)
        check (await bus.publish("redischronos:shared", "two")) == 1
        await sleepAsync(20.milliseconds)
        check first == 1
        check second == 2
        await bus.unsubscribe(secondSubscription)
        await bus.close()
      waitFor exercise()

    test "subscriber reconnects, applies offline changes, and resubscribes":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        var states: seq[ConnectionState]
        let stateHandler: StateHandler =
          proc(state: ConnectionState): Future[void] {.async.} =
            states.add(state)
        bus.onStateChange(stateHandler)

        var firstMessages, offlineMessages: int
        let firstHandler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            inc firstMessages
        let offlineHandler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            inc offlineMessages
        discard await bus.subscribe("redischronos:reconnect", firstHandler)
        await bus.disconnectSubscriberForTest()

        for _ in 0 ..< 50:
          if csDisconnected in states:
            break
          await sleepAsync(10.milliseconds)
        check csDisconnected in states

        discard await bus.subscribe(
          "redischronos:offline-change",
          offlineHandler
        )
        for _ in 0 ..< 100:
          if states.len >= 3 and states[^1] == csConnected:
            break
          await sleepAsync(10.milliseconds)
        check states[^1] == csConnected

        discard await bus.publish("redischronos:reconnect", "one")
        discard await bus.publish("redischronos:offline-change", "two")
        await sleepAsync(30.milliseconds)
        check firstMessages == 1
        check offlineMessages == 1
        await bus.close()
      waitFor exercise()

    test "close during reconnect backoff is bounded":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        var disconnected = false
        let stateHandler: StateHandler =
          proc(state: ConnectionState): Future[void] {.async.} =
            if state == csDisconnected:
              disconnected = true
        bus.onStateChange(stateHandler)
        await bus.disconnectSubscriberForTest()
        for _ in 0 ..< 50:
          if disconnected:
            break
          await sleepAsync(10.milliseconds)
        check disconnected
        await bus.close().wait(500.milliseconds)
      waitFor exercise()
