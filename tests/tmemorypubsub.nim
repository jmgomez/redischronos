import std/unittest
import chronos
import redischronos
import redischronos/memorypubsub

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

  test "close owns one cleanup task when a caller is cancelled":
    proc exercise() {.async.} =
      var options = defaultBackendOptions()
      options.operationTimeout = 50.milliseconds
      let bus = await openPubSub("mem://", options)
      let handler: StateHandler =
        proc(state: ConnectionState): Future[void] {.async.} =
          await sleepAsync(1.hours)
      bus.onStateChange(handler)
      let first = bus.close()
      let second = bus.close()
      first.cancelSoon()
      await first
      await second.wait(200.milliseconds)
      await bus.close()
    waitFor exercise()

  test "close retains and cancels a worker retired by unsubscribe":
    proc exercise() {.async.} =
      let bus = await openPubSub()
      let started = newFuture[void]("retiring worker started")
      var terminated = false
      let handler: MessageHandler =
        proc(channel, payload: string): Future[void] {.async.} =
          started.complete()
          try:
            await sleepAsync(1.hours)
          finally:
            terminated = true
      let subscription = await bus.subscribe("events", handler)
      discard await bus.publish("events", "payload")
      await started
      await bus.unsubscribe(subscription)
      await bus.close()
      check terminated
    waitFor exercise()

  test "completed retiring workers compact without waiting for close":
    proc exercise() {.async.} =
      let bus = await openPubSub()
      let started = newFuture[void]("compact worker started")
      let release = newFuture[void]("compact worker release")
      let subscription = await bus.subscribe(
        "events",
        proc(channel, payload: string): Future[void] {.async.} =
          started.complete()
          await release
      )
      discard await bus.publish("events", "payload")
      await started
      await bus.unsubscribe(subscription)
      release.complete()
      await sleepAsync(10.milliseconds)
      check bus.deliveryStateCountsForTest() == (0, 0, 0)
      await bus.close()
    waitFor exercise()

  test "observer replacement is ordered and post-close registration is inert":
    proc exercise() {.async.} =
      let bus = await openPubSub()
      var firstCalls, secondCalls, postCloseCalls: int
      bus.onStateChange(
        proc(state: ConnectionState): Future[void] {.async.} =
          inc firstCalls
      )
      bus.onStateChange(
        proc(state: ConnectionState): Future[void] {.async.} =
          inc secondCalls
      )
      await sleepAsync(10.milliseconds)
      await bus.close()
      bus.onStateChange(
        proc(state: ConnectionState): Future[void] {.async.} =
          inc postCloseCalls
      )
      await sleepAsync(10.milliseconds)
      check firstCalls + secondCalls >= 2
      check postCloseCalls == 0
    waitFor exercise()

  test "a callback can close its bus without deadlocking":
    proc exercise() {.async.} =
      let bus = await openPubSub()
      let returned = newFuture[void]("callback close returned")
      discard await bus.subscribe(
        "events",
        proc(channel, payload: string): Future[void] {.async.} =
          await sleepAsync(1.milliseconds)
          await bus.close()
          returned.complete()
      )
      discard await bus.publish("events", "payload")
      await returned.wait(100.milliseconds)
      await bus.close().wait(100.milliseconds)
      expect BackendClosedError:
        discard await bus.publish("events", "after close")
    waitFor exercise()

  test "a yielding state observer can await close":
    proc exercise() {.async.} =
      let bus = await openPubSub()
      let returned = newFuture[void]("state callback close returned")
      bus.onStateChange(
        proc(state: ConnectionState): Future[void] {.async.} =
          if state == csConnected and not returned.finished:
            await sleepAsync(1.milliseconds)
            await bus.close()
            returned.complete()
      )
      await returned.wait(100.milliseconds)
      await bus.close()
    waitFor exercise()
