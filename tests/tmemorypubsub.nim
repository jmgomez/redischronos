import std/unittest
import chronos
import redischronos

import ./contract/pubsubcontract

proc openMemoryPubSub(options: BackendOptions): Future[PubSub] {.gcsafe.} =
  openPubSub("mem://", options)

pubSubContractSuite("memory", openMemoryPubSub)

suite "memory Pub/Sub internals":
  test "callbacks may mutate subscriptions safely":
    proc exercise() {.async.} =
      let bus = await openPubSub()
      var calls: seq[string]
      var original: Subscription
      let replacement: MessageHandler =
        proc(channel, payload: string): Future[void] {.async.} =
          calls.add("replacement:" & payload)
      let handler: MessageHandler =
        proc(channel, payload: string): Future[void] {.async.} =
          calls.add("original:" & payload)
          await bus.unsubscribe(original)
          discard await bus.subscribe(channel, replacement)
      original = await bus.subscribe("events", handler)
      discard await bus.publish("events", "first")
      await sleepAsync(10.milliseconds)
      discard await bus.publish("events", "second")
      await sleepAsync(10.milliseconds)
      check calls == @["original:first", "replacement:second"]
      await bus.close()
    waitFor exercise()

  test "close cancels and drains an active handler":
    proc exercise() {.async.} =
      let bus = await openPubSub()
      var terminated = false
      let handler: MessageHandler =
        proc(channel, payload: string): Future[void] {.async.} =
          try:
            await sleepAsync(1.hours)
          finally:
            terminated = true
      discard await bus.subscribe("events", handler)
      discard await bus.publish("events", "message")
      await bus.close()
      check terminated
    waitFor exercise()

  test "delivery queues are bounded and unsubscribe discards queued payloads":
    proc exercise() {.async.} =
      var options = defaultBackendOptions()
      options.pubSubMaxPendingMessages = 2
      let bus = await openPubSub("mem://", options)
      let release = newFuture[void]("release slow handler")
      var received: seq[string]
      let handler: MessageHandler =
        proc(channel, payload: string): Future[void] {.async.} =
          received.add(payload)
          if payload == "first":
            await release
      let subscription = await bus.subscribe("events", handler)
      for payload in ["first", "second", "third", "dropped"]:
        discard await bus.publish("events", payload)
      await bus.unsubscribe(subscription)
      release.complete()
      await sleepAsync(10.milliseconds)
      check received == @["first"]
      await bus.close()
    waitFor exercise()

  test "a stuck state observer cannot block close":
    proc exercise() {.async.} =
      var options = defaultBackendOptions()
      options.operationTimeout = 20.milliseconds
      let bus = await openPubSub("mem://", options)
      let handler: StateHandler =
        proc(state: ConnectionState): Future[void] {.async.} =
          await sleepAsync(1.hours)
      bus.onStateChange(handler)
      await bus.close().wait(100.milliseconds)
    waitFor exercise()
