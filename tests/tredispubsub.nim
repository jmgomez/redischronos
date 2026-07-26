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
    test "other Redis clients do not affect the portable publish count":
      proc exercise() {.async.} =
        let first = await openPubSub(getEnv("REDIS_TEST_URL"))
        let other = await openPubSub(getEnv("REDIS_TEST_URL"))
        let handler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            discard
        let local = await first.subscribe("redischronos:counts", handler)
        let external = await other.subscribe("redischronos:counts", handler)
        check (await first.publish("redischronos:counts", "payload")) == 1
        await first.unsubscribe(local)
        await other.unsubscribe(external)
        await first.close()
        await other.close()
      waitFor exercise()

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
        check (await bus.publish("redischronos:shared", "one")) == 2
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

    test "state observers cannot block reconnect or close":
      proc exercise() {.async.} =
        var options = defaultBackendOptions()
        options.operationTimeout = 30.milliseconds
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"), options)
        let stuck: StateHandler =
          proc(state: ConnectionState): Future[void] {.async.} =
            await sleepAsync(1.hours)
        bus.onStateChange(stuck)
        await bus.disconnectSubscriberForTest()
        await sleepAsync(150.milliseconds)
        await bus.close().wait(150.milliseconds)
      waitFor exercise()

    test "injected reconnect jitter is capped and exercised":
      proc exercise() {.async.} =
        var calls: seq[int]
        var options = defaultBackendOptions()
        options.reconnectJitterSource =
          proc(maxInclusive: int): int {.gcsafe, raises: [].} =
            calls.add(maxInclusive)
            maxInclusive + 100
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"), options)
        await bus.disconnectSubscriberForTest()
        for _ in 0 ..< 100:
          if calls.len > 0:
            break
          await sleepAsync(10.milliseconds)
        check calls.len > 0
        check calls[0] > 0
        await bus.close()
      waitFor exercise()

    test "callback mutations use stable snapshots":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        var calls: seq[string]
        var self, peer: Subscription
        let replacement: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            calls.add("replacement:" & payload)
        let peerHandler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            calls.add("peer:" & payload)
        let mutating: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            calls.add("self:" & payload)
            await bus.unsubscribe(self)
            await bus.unsubscribe(peer)
            discard await bus.subscribe(channel, replacement)
        self = await bus.subscribe("redischronos:mutations", mutating)
        peer = await bus.subscribe("redischronos:mutations", peerHandler)
        discard await bus.publish("redischronos:mutations", "first")
        await sleepAsync(30.milliseconds)
        discard await bus.publish("redischronos:mutations", "second")
        await sleepAsync(30.milliseconds)
        check "self:first" in calls
        check "peer:second" notin calls
        check "replacement:second" in calls
        await bus.close()
      waitFor exercise()

    test "a callback can close its bus without deadlocking":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        let returned = newFuture[void]("Redis callback close returned")
        discard await bus.subscribe(
          "redischronos:callback-close",
          proc(channel, payload: string): Future[void] {.async.} =
            await sleepAsync(1.milliseconds)
            await bus.close()
            returned.complete()
        )
        discard await bus.publish(
          "redischronos:callback-close", "payload"
        )
        await returned.wait(200.milliseconds)
        await bus.close().wait(200.milliseconds)
        expect BackendClosedError:
          discard await bus.publish(
            "redischronos:callback-close", "after close"
          )
      waitFor exercise()

    test "a yielding state observer can await close":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        let returned = newFuture[void]("Redis state callback close returned")
        bus.onStateChange(
          proc(state: ConnectionState): Future[void] {.async.} =
            if state == csConnected and not returned.finished:
              await sleepAsync(1.milliseconds)
              await bus.close()
              returned.complete()
        )
        await returned.wait(200.milliseconds)
        await bus.close()
      waitFor exercise()

    test "message and state callback helpers concurrently await close":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        let release = newFuture[void]("release Redis close helpers")
        let bothStarted = newFuture[void]("both Redis callbacks started")
        let bothReturned = newFuture[void]("both Redis callbacks returned")
        var started, returned: int
        proc closeViaHelper() {.async.} =
          await sleepAsync(1.milliseconds)
          await bus.close()
        proc runClose() {.async.} =
          inc started
          if started == 2:
            bothStarted.complete()
          await release
          await closeViaHelper()
          inc returned
          if returned == 2:
            bothReturned.complete()
        bus.onStateChange(
          proc(state: ConnectionState): Future[void] {.async.} =
            if state == csConnected:
              await runClose()
        )
        discard await bus.subscribe(
          "redischronos:concurrent-close",
          proc(channel, payload: string): Future[void] {.async.} =
            await runClose()
        )
        discard await bus.publish(
          "redischronos:concurrent-close", "payload"
        )
        await bothStarted.wait(200.milliseconds)
        release.complete()
        await bothReturned.wait(200.milliseconds)
        await bus.close().wait(200.milliseconds)
        check bus.closeCallerCountForTest() == 0
      waitFor exercise()

    test "terminal close calls retain no caller ancestry":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        await bus.close()
        for _ in 0 ..< 10_000:
          await bus.close()
        check bus.closeCallerCountForTest() == 0
      waitFor exercise()

    test "bounded queues and retiring workers remain owned through close":
      proc exercise() {.async.} =
        var options = defaultBackendOptions()
        options.pubSubMaxPendingMessages = 2
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"), options)
        let started = newFuture[void]("Redis slow handler started")
        var terminated = false
        var calls = 0
        let handler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            inc calls
            if not started.finished:
              started.complete()
            try:
              await sleepAsync(1.hours)
            finally:
              terminated = true
        let subscription =
          await bus.subscribe("redischronos:bounded", handler)
        discard await bus.publish("redischronos:bounded", "first")
        await started
        for payload in ["second", "third", "dropped"]:
          discard await bus.publish("redischronos:bounded", payload)
        await sleepAsync(30.milliseconds)
        await bus.unsubscribe(subscription)
        let first = bus.close()
        let second = bus.close()
        first.cancelSoon()
        await first
        await second
        check calls == 1
        check terminated
      waitFor exercise()

    test "completed retiring workers compact without waiting for close":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        let started = newFuture[void]("Redis compact worker started")
        let release = newFuture[void]("Redis compact worker release")
        let subscription = await bus.subscribe(
          "redischronos:compact",
          proc(channel, payload: string): Future[void] {.async.} =
            started.complete()
            await release
        )
        discard await bus.publish("redischronos:compact", "payload")
        await started
        await bus.unsubscribe(subscription)
        release.complete()
        await sleepAsync(20.milliseconds)
        check bus.deliveryStateCountsForTest() == (0, 0, 0)
        await bus.close()
      waitFor exercise()

    test "observer replacement and post-close registration own no stray task":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
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

    test "queued subscriber writer cancellation preserves the connection":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        let acquired = newFuture[void]("subscriber lock acquired")
        let release = newFuture[void]("subscriber lock release")
        let holder =
          bus.holdSubscriberWriteLockForTest(acquired, release)
        await acquired
        let handler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            discard
        let queued = bus.subscribe("redischronos:cancelled-writer", handler)
        queued.cancelSoon()
        expect CancelledError:
          discard await queued
        release.complete()
        await holder
        let subscription =
          await bus.subscribe("redischronos:after-cancel", handler)
        check (await bus.publish(
          "redischronos:after-cancel", "payload"
        )) == 1
        await bus.unsubscribe(subscription)
        await bus.close()
      waitFor exercise()

    test "an expired subscriber budget starts no write and poisons no lock":
      proc exercise() {.async.} =
        let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
        let handler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            discard
        bus.setOperationTimeoutForTest(0.nanoseconds)
        expect BackendTimeoutError:
          discard await bus.subscribe("redischronos:expired", handler)
        bus.setOperationTimeoutForTest(1.seconds)
        let subscription =
          await bus.subscribe("redischronos:after-expired", handler)
        check (await bus.publish(
          "redischronos:after-expired", "payload"
        )) == 1
        await bus.unsubscribe(subscription)
        await bus.close()
      waitFor exercise()
