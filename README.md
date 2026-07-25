# redischronos

`redischronos` is a small Chronos-native key/value and Pub/Sub library for Nim.
Applications use backend-neutral `KvStore` and `PubSub` handles. The in-memory
and Redis backends implement the same behavioral contracts, so deployment
configuration—not imports or call sites—selects the backend.

## Installation

```text
nimble install redischronos
```

The package requires Nim 2.2 or newer and Chronos 4.x. It does not depend on
`asyncdispatch` or a system Redis client.

## Key/value storage

```nim
import std/[options, os]
import chronos
import redischronos

proc main() {.async.} =
  let store = await openKvStore(getEnv("REDISCHRONOS_URL", "mem://"))
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

An empty URL and `mem://` select the in-memory implementation. A
`redis://` URL selects Redis; unknown schemes fail explicitly. Supported forms
include:

```text
redis://host
redis://host:6379/2
redis://:password@host/2
redis://username:password@host/2
redis://[::1]:6379/2
```

Usernames and passwords may be percent-encoded. `rediss://` is not supported.
Credentials and full URLs are never included in library errors.

Memory state and subscriptions are local to one process and one Chronos event
loop. Backend objects may interleave on that loop, but cross-thread sharing is
outside the contract. Always close stores and buses; close is asynchronous and
idempotent.

Redis KV uses one serialized command connection. Redis Pub/Sub uses a separate
serialized publish connection and one dedicated subscriber connection.
Commands are not pipelined or replayed after an ambiguous connection failure.
The next operation reconnects when possible.

All connect, handshake, command, and subscriber writes are bounded by
`BackendOptions.connectTimeout` and `operationTimeout`. Subscriber disconnects
produce `csDisconnected`; reconnect uses capped exponential backoff with
jitter and automatically restores current subscriptions. Messages published
during downtime are ephemeral and are not recovered. Applications choose
their own fail-open or fail-closed policy.

## Compatibility

| Component | Supported |
| --- | --- |
| Nim | 2.2.x and newer compatible 2.x releases |
| Memory management | refc and ORC |
| Chronos | 4.x |
| Redis | Redis 7, RESP2 over `redis://` |
| TLS | Not currently supported |

## Configuration-only switching

[`examples/backendswitch.nim`](examples/backendswitch.nim) is deliberately
backend-neutral. Compile it once and run the same executable against either
backend:

```bash
nim c --path:src examples/backendswitch.nim
REDISCHRONOS_URL=mem:// ./examples/backendswitch
REDISCHRONOS_URL=redis://127.0.0.1:6379/15 ./examples/backendswitch
```

Both runs produce the same contract-observable output. Only the URL changes.

## Testing

Set `NIM_BIN` if Nim is not on `PATH`, then run:

```bash
nimble test
nimble testRefc
nimble testOrc
nimble testFile tests/tmemorykv.nim
REDIS_TEST_URL=redis://127.0.0.1:6379/15 nimble testRedisRefc
REDIS_TEST_URL=redis://127.0.0.1:6379/15 nimble testRedisOrc
```

The reusable factory-driven contracts live under `tests/contract` so every
backend is held to the same observable behavior. Redis integration tasks fail
immediately when `REDIS_TEST_URL` is absent; CI never silently skips Redis.
