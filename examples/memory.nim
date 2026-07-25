import std/options
import chronos
import redischronos

proc main() {.async.} =
  let store = await openKvStore("mem://")
  let bus = await openPubSub("mem://")
  let handler: MessageHandler =
    proc(channel, payload: string): Future[void] {.async.} =
      echo channel, ": ", payload

  try:
    await store.set("greeting", "hello", ttlSeconds = 60)
    let greeting = await store.get("greeting")
    if greeting.isSome:
      echo greeting.get

    let subscription = await bus.subscribe("events", handler)
    discard await bus.publish("events", "ready")
    await bus.unsubscribe(subscription)
  finally:
    await bus.close()
    await store.close()

waitFor main()
