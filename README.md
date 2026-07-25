# redischronos

`redischronos` is a small Chronos-native key/value and Pub/Sub library for Nim.
Applications use backend-neutral `KvStore` and `PubSub` handles. Version 0.1
ships the process-local memory backend; Redis support will use the same API.

## Installation

```text
nimble install redischronos
```

The package requires Nim 2.2 or newer and Chronos 4.x. It does not depend on
`asyncdispatch` or a system Redis client.

## Key/value storage

```nim
import std/options
import chronos
import redischronos

proc main() {.async.} =
  let store = await openKvStore("mem://")
  try:
    await store.set("greeting", "hello")
    let value = await store.get("greeting")
    if value.isSome:
      echo value.get

    await store.set("session", "active", ttlSeconds = 60)
    echo await store.increment("requests")
  finally:
    await store.close()

waitFor main()
```

Strings are binary-safe. A zero TTL means no expiry; negative TTL values raise
`InvalidArgumentError`. The memory backend is bounded by
`BackendOptions.memoryMaxEntries` and evicts the least recently used entry
when capacity is exceeded.

## Pub/Sub

```nim
import chronos
import redischronos

proc main() {.async.} =
  let bus = await openPubSub("mem://")
  let handler: MessageHandler =
    proc(channel, payload: string): Future[void] {.async.} =
      echo channel, ": ", payload

  try:
    let subscription = await bus.subscribe("events", handler)
    discard await bus.publish("events", "ready")
    await bus.unsubscribe(subscription)
  finally:
    await bus.close()

waitFor main()
```

Publishing is ephemeral and at-most-once. Publish completion means the backend
accepted the message; handlers run asynchronously. Delivery order is preserved
per subscription.

## Errors and connection state

All library failures derive from `RedisChronosError`. Chronos cancellation is
not wrapped. Handler failures are isolated and can be observed without logging
message payloads:

```nim
var options = defaultBackendOptions()
options.onHandlerError =
  proc(error: HandlerError) {.gcsafe, raises: [].} =
    echo "handler failed on ", error.channel

let bus = await openPubSub("mem://", options)
bus.onStateChange(
  proc(state: ConnectionState): Future[void] {.async.} =
    echo state
)
```

The memory backend reports `csConnected` immediately after state-handler
registration and `csClosed` during shutdown. It never reports a synthetic
disconnect.

## Backend and concurrency boundaries

An empty URL and `mem://` select the in-memory implementation. Unknown schemes
fail explicitly. A later Redis-enabled release will accept `redis://...`
without requiring different imports or call sites.

Memory state and subscriptions are local to one process and one Chronos event
loop. Backend objects may interleave on that loop, but cross-thread sharing is
outside the contract. Always close stores and buses; close is asynchronous and
idempotent.

## Testing

Set `NIM_BIN` if Nim is not on `PATH`, then run:

```bash
nimble test
nimble testRefc
nimble testOrc
nimble testFile tests/tmemorykv.nim
```

The reusable factory-driven contracts live under `tests/contract` so every
backend is held to the same observable behavior.
