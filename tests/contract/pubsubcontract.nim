import std/unittest
import chronos
import redischronos

type PubSubFactory* =
  proc(options: BackendOptions): Future[PubSub] {.gcsafe.}

proc probePubSubContract*(factory: PubSubFactory): Future[seq[string]] {.async.} =
  let bus = await factory(defaultBackendOptions())
  var deliveries = 0
  let delivered = newFuture[void]("probe Pub/Sub delivery")
  let handler: MessageHandler =
    proc(channel, payload: string): Future[void] {.async.} =
      inc deliveries
      if deliveries == 2:
        delivered.complete()
  if (await bus.publish("probe", "none")) != 0:
    result.add("empty publish count")
  try:
    discard await bus.subscribe("", handler)
    result.add("invalid channel")
  except InvalidArgumentError:
    discard
  except CatchableError:
    result.add("invalid channel")
  let first = await bus.subscribe("probe", handler)
  discard await bus.subscribe("probe", handler)
  if (await bus.publish("probe", "message")) != 2:
    result.add("local publish count")
  try:
    await delivered.wait(1.seconds)
  except AsyncTimeoutError:
    discard
  if deliveries != 2:
    result.add("delivery")
  await bus.unsubscribe(first)
  if (await bus.publish("probe", "again")) != 1:
    result.add("unsubscribe")
  let stateSeen = newFuture[void]("probe state")
  bus.onStateChange(
    proc(state: ConnectionState): Future[void] {.async.} =
      if not stateSeen.finished:
        stateSeen.complete()
  )
  try:
    await stateSeen.wait(50.milliseconds)
  except AsyncTimeoutError:
    result.add("state notification")
  await bus.close()
  try:
    discard await bus.publish("probe", "closed")
    result.add("closed lifecycle")
  except BackendClosedError:
    discard

template pubSubContractSuite*(backendName: string, factory: PubSubFactory) =
  suite backendName & " Pub/Sub contract":
    test "exact routing, counts, ordering, and unsubscribe":
      proc exercise() {.async.} =
        let bus = await factory(defaultBackendOptions())
        var received: seq[string]
        let delivered = newFuture[void]("contract ordered delivery")
        let handler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            received.add(channel & ":" & payload)
            if received.len == 2:
              delivered.complete()
        let subscription = await bus.subscribe("events", handler)
        discard await bus.subscribe("other", handler)
        check (await bus.publish("events", "1")) == 1
        check (await bus.publish("events", "2")) == 1
        check (await bus.publish("none", "0")) == 0
        await delivered.wait(1.seconds)
        check received == @["events:1", "events:2"]
        await bus.unsubscribe(subscription)
        await bus.unsubscribe(subscription)
        check (await bus.publish("events", "3")) == 0
        await bus.close()
      waitFor exercise()

    test "publish reports active local subscriptions":
      proc exercise() {.async.} =
        let bus = await factory(defaultBackendOptions())
        let handler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            discard
        check (await bus.publish("counts", "zero")) == 0
        let first = await bus.subscribe("counts", handler)
        check (await bus.publish("counts", "one")) == 1
        let second = await bus.subscribe("counts", handler)
        check (await bus.publish("counts", "two")) == 2
        await bus.unsubscribe(first)
        check (await bus.publish("counts", "one-again")) == 1
        await bus.unsubscribe(second)
        check (await bus.publish("counts", "zero-again")) == 0
        await bus.close()
      waitFor exercise()

    test "handler failures are isolated":
      proc exercise() {.async.} =
        var errors = 0
        var observedCause = ""
        let isolated = newFuture[void]("contract handler isolation")
        var healthy = 0
        var options = defaultBackendOptions()
        options.onHandlerError =
          proc(error: HandlerError) {.gcsafe, raises: [].} =
            inc errors
            observedCause = error.cause.msg
            if healthy == 1 and not isolated.finished:
              isolated.complete()
        let bus = await factory(options)
        let failing: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            raise newException(ValueError, payload)
        let handler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            inc healthy
            if errors == 1 and not isolated.finished:
              isolated.complete()
        discard await bus.subscribe("events", failing)
        discard await bus.subscribe("events", handler)
        discard await bus.publish("events", "secret")
        await isolated.wait(1.seconds)
        check errors == 1
        check healthy == 1
        check observedCause != "secret"
        await bus.close()
      waitFor exercise()

    test "state and lifecycle are portable":
      proc exercise() {.async.} =
        let bus = await factory(defaultBackendOptions())
        var states: seq[ConnectionState]
        let connected = newFuture[void]("contract connected state")
        let handler: StateHandler =
          proc(state: ConnectionState): Future[void] {.async.} =
            states.add(state)
            if state == csConnected and not connected.finished:
              connected.complete()
        bus.onStateChange(handler)
        await connected.wait(1.seconds)
        await bus.close()
        await bus.close()
        check states[0] == csConnected
        check states[^1] == csClosed
        expect BackendClosedError:
          discard await bus.publish("events", "payload")
      waitFor exercise()
