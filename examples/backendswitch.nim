import std/[options, os]
import chronos
import redischronos

proc main() {.async.} =
  let backendUrl = getEnv("REDISCHRONOS_URL", "mem://")
  let store = await openKvStore(backendUrl)
  let bus = await openPubSub(backendUrl)
  var delivered = newFuture[string]("backend switch delivery")
  let handler: MessageHandler =
    proc(channel, payload: string): Future[void] {.async.} =
      if not delivered.finished():
        delivered.complete(channel & "=" & payload)

  try:
    discard await store.delete("redischronos:example:counter")
    echo "missing=", (await store.get("redischronos:example:missing")).isNone
    await store.set("redischronos:example:value", "ready")
    echo "value=", (await store.get("redischronos:example:value")).get
    echo "counter=", await store.increment("redischronos:example:counter")

    let subscription =
      await bus.subscribe("redischronos:example:events", handler)
    echo "receivers=", await bus.publish(
      "redischronos:example:events",
      "ready"
    )
    echo "message=", await delivered.wait(2.seconds)
    await bus.unsubscribe(subscription)
  finally:
    discard await store.delete("redischronos:example:value")
    discard await store.delete("redischronos:example:counter")
    await bus.close()
    await store.close()

waitFor main()
