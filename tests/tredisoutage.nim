import std/[os, osproc, unittest]
import chronos
import redischronos

when defined(redisIntegration):
  if getEnv("REDIS_TEST_CONTAINER").len > 0:
    suite "real Redis sustained outage":
      test "active subscriptions reconcile across stop and restart":
        proc exercise() {.async.} =
          let redisContainer = getEnv("REDIS_TEST_CONTAINER")
          proc restoreAuthentication() =
            let base = "docker exec " & quoteShell(redisContainer) &
              " redis-cli"
            check execCmd(
              base & " CONFIG SET requirepass audit-password"
            ) == 0
            check execCmd(
              base & " -a audit-password ACL SETUSER audituser on " &
              quoteShell(">acl-password") & " " &
              quoteShell("~*") & " " & quoteShell("+@all")
            ) == 0
          let bus = await openPubSub(getEnv("REDIS_TEST_URL"))
          var states: seq[ConnectionState]
          var activeMessages, removedMessages: int
          bus.onStateChange(
            proc(state: ConnectionState): Future[void] {.async.} =
              states.add(state)
          )
          let activeHandler: MessageHandler =
            proc(channel, payload: string): Future[void] {.async.} =
              inc activeMessages
          let removedHandler: MessageHandler =
            proc(channel, payload: string): Future[void] {.async.} =
              inc removedMessages
          discard await bus.subscribe("redischronos:outage:active", activeHandler)
          let removed =
            await bus.subscribe("redischronos:outage:removed", removedHandler)

          try:
            check execCmd("docker stop " & quoteShell(redisContainer)) == 0
            for _ in 0 ..< 100:
              if csDisconnected in states:
                break
              await sleepAsync(20.milliseconds)
            check csDisconnected in states

            await bus.unsubscribe(removed)
            discard await bus.subscribe(
              "redischronos:outage:added",
              activeHandler
            )
            await sleepAsync(350.milliseconds)
            check execCmd("docker start " & quoteShell(redisContainer)) == 0
            restoreAuthentication()

            for _ in 0 ..< 200:
              if states.len > 0 and states[^1] == csConnected:
                break
              await sleepAsync(25.milliseconds)
            check states[^1] == csConnected
            check (await bus.publish(
              "redischronos:outage:active", "one"
            )) == 1
            check (await bus.publish(
              "redischronos:outage:added", "two"
            )) == 1
            await sleepAsync(50.milliseconds)
            check activeMessages == 2
            check removedMessages == 0

            check execCmd("docker stop " & quoteShell(redisContainer)) == 0
            await bus.close().wait(1.seconds)
          finally:
            discard execCmd("docker start " & quoteShell(redisContainer))
            restoreAuthentication()
            await bus.close()
        waitFor exercise()

  if getEnv("REDIS_TEST_RESTART_EXECUTABLE").len > 0:
    suite "local real Redis process outage":
      test "subscriber recovers after a sustained process restart":
        proc exercise() {.async.} =
          let executable = getEnv("REDIS_TEST_RESTART_EXECUTABLE")
          let port = getEnv("REDIS_TEST_RESTART_PORT", "6397")
          proc launch(): Process =
            startProcess(executable, args = [
              "--port", port,
              "--save", "",
              "--appendonly", "no"
            ], options = {poUsePath})
          proc stop(process: Process) =
            process.terminate()
            discard process.waitForExit(5000)
            process.close()

          var redis = launch()
          var bus: PubSub
          try:
            for _ in 0 ..< 100:
              try:
                bus = await openPubSub("redis://127.0.0.1:" & port & "/15")
                break
              except BackendConnectionError:
                await sleepAsync(20.milliseconds)
            check bus != nil

            var states: seq[ConnectionState]
            var messages = 0
            bus.onStateChange(
              proc(state: ConnectionState): Future[void] {.async.} =
                states.add(state)
            )
            let handler: MessageHandler =
              proc(channel, payload: string): Future[void] {.async.} =
                inc messages
            discard await bus.subscribe("redischronos:local-outage", handler)

            redis.stop()
            for _ in 0 ..< 100:
              if csDisconnected in states:
                break
              await sleepAsync(20.milliseconds)
            check csDisconnected in states
            await sleepAsync(350.milliseconds)

            redis = launch()
            for _ in 0 ..< 200:
              if states.len > 0 and states[^1] == csConnected:
                break
              await sleepAsync(25.milliseconds)
            check states[^1] == csConnected
            check (await bus.publish(
              "redischronos:local-outage", "recovered"
            )) == 1
            await sleepAsync(50.milliseconds)
            check messages == 1

            redis.stop()
            await bus.close().wait(1.seconds)
            bus = nil
          finally:
            if bus != nil:
              await bus.close()
            if redis.running:
              redis.stop()
        waitFor exercise()
