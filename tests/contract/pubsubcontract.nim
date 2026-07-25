import std/unittest
import chronos
import redischronos

type PubSubFactory* =
  proc(options: BackendOptions): Future[PubSub] {.gcsafe.}

proc settleContract() {.async.} =
  await sleepAsync(10.milliseconds)

template pubSubContractSuite*(backendName: string, factory: PubSubFactory) =
  suite backendName & " Pub/Sub contract":
    test "exact routing, counts, ordering, and unsubscribe":
      proc exercise() {.async.} =
        let bus = await factory(defaultBackendOptions())
        var received: seq[string]
        let handler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            received.add(channel & ":" & payload)
        let subscription = await bus.subscribe("events", handler)
        discard await bus.subscribe("other", handler)
        check (await bus.publish("events", "1")) == 1
        check (await bus.publish("events", "2")) == 1
        check (await bus.publish("none", "0")) == 0
        await settleContract()
        check received == @["events:1", "events:2"]
        await bus.unsubscribe(subscription)
        await bus.unsubscribe(subscription)
        check (await bus.publish("events", "3")) == 0
        await bus.close()
      waitFor exercise()

    test "handler failures are isolated":
      proc exercise() {.async.} =
        var errors = 0
        var options = defaultBackendOptions()
        options.onHandlerError =
          proc(error: HandlerError) {.gcsafe, raises: [].} =
            inc errors
        let bus = await factory(options)
        let failing: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            raise newException(ValueError, "failed")
        var healthy = 0
        let handler: MessageHandler =
          proc(channel, payload: string): Future[void] {.async.} =
            inc healthy
        discard await bus.subscribe("events", failing)
        discard await bus.subscribe("events", handler)
        discard await bus.publish("events", "secret")
        await settleContract()
        check errors == 1
        check healthy == 1
        await bus.close()
      waitFor exercise()

    test "state and lifecycle are portable":
      proc exercise() {.async.} =
        let bus = await factory(defaultBackendOptions())
        var states: seq[ConnectionState]
        let handler: StateHandler =
          proc(state: ConnectionState): Future[void] {.async.} =
            states.add(state)
        bus.onStateChange(handler)
        await settleContract()
        await bus.close()
        await bus.close()
        check states[0] == csConnected
        check states[^1] == csClosed
        expect BackendClosedError:
          discard await bus.publish("events", "payload")
      waitFor exercise()
