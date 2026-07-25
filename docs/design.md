# redischronos design

**Status:** Approved for implementation  
**Target:** Nim 2.2+, Chronos 4.x  
**Delivery:** Phase 1 memory backends, then Phase 2 Redis backends behind the
same public contracts

## 1. Purpose

`redischronos` is a small asynchronous key/value and Pub/Sub library. It
provides two backend families:

1. Process-local memory implementations requiring no service.
2. Redis implementations using a Chronos-native RESP2 client.

Applications program against `KvStore` and `PubSub`. Backend selection is
configuration:

```text
"" or "mem://"  -> memory
"redis://..."   -> Redis
```

When both phases are complete, activating Redis requires deploying Redis and
setting a URL. It must not require library or application code changes.

## 2. Design principles

1. **One contract, two backends.** Every portable behavior runs through the
   same parameterized contract suite.
2. **Memory first, not memory-only architecture.** Phase 1 is immediately
   useful, but all seams are deliberately compatible with Phase 2.
3. **Mechanisms, not policies.** The library supplies storage and transport;
   consumers decide key schemas, serialization, invalidation, and degradation.
4. **Small surface.** Prefer five KV operations and four Pub/Sub lifecycle
   operations over a framework.
5. **Explicit lifecycle.** Opening and closing resources are async,
   deterministic, and idempotent.
6. **Bounded failure.** Network operations time out and return typed errors.
7. **No unsafe retry.** A command with an ambiguous outcome is never replayed
   automatically.

## 3. Scope

### 3.1 Included

- String key/value storage: get, set, delete, exists, atomic increment.
- Optional per-entry TTL in whole seconds.
- Bounded in-memory capacity with deterministic LRU eviction.
- Pub/Sub subscribe, unsubscribe, publish, and connection-state hooks.
- Empty/`mem://` and `redis://` URL factories.
- RESP2 client over Chronos streams.
- Redis AUTH (password and ACL username/password) and database selection.
- Deadlines, typed errors, reconnect-on-next-command, subscriber
  reconnection, and resubscription.
- refc and ORC support.
- Reusable backend contract tests.

### 3.2 Explicitly excluded

- Cache invalidation policies, serialization, namespacing, or domain schemas.
- Distributed locks, queues, rate limiters, streams, and transactions.
- Redis Cluster, Sentinel, RESP3, client-side caching, and Lua.
- Redis persistence or deployment management.
- Cross-thread sharing of a backend object.
- Automatic replay of commands after an uncertain network failure.
- TLS/`rediss://` in the initial stable release.

These exclusions keep the first stable release small. They require a separate
design decision rather than silently expanding Phase 2.

## 4. Public API

The exact Nim syntax may be adjusted during the first test, but the behavioral
surface is fixed by this section.

```nim
type
  BackendKind* = enum
    bkMemory, bkRedis

  BackendOptions* = object
    connectTimeout*: Duration
    operationTimeout*: Duration
    memoryMaxEntries*: int
    onHandlerError*: HandlerErrorObserver

  KvStore* = ref object of RootObj
  PubSub* = ref object of RootObj
  Subscription* = ref object

  ConnectionState* = enum
    csConnecting, csConnected, csDisconnected, csClosed

  MessageHandler* =
    proc(channel, payload: string): Future[void] {.gcsafe.}

  StateHandler* =
    proc(state: ConnectionState): Future[void] {.gcsafe.}

  HandlerError* = object
    channel*: string
    subscriptionId*: uint64
    cause*: ref CatchableError

  HandlerErrorObserver* =
    proc(error: HandlerError) {.gcsafe, raises: [].}

proc defaultBackendOptions*(): BackendOptions

proc openKvStore*(
  url = "mem://",
  options = defaultBackendOptions()
): Future[KvStore]

proc openPubSub*(
  url = "mem://",
  options = defaultBackendOptions()
): Future[PubSub]

method get*(store: KvStore, key: string): Future[Option[string]]
method set*(
  store: KvStore,
  key, value: string,
  ttlSeconds = 0
): Future[void]
method delete*(store: KvStore, key: string): Future[bool]
method exists*(store: KvStore, key: string): Future[bool]
method increment*(store: KvStore, key: string): Future[int64]
method close*(store: KvStore): Future[void]

method subscribe*(
  bus: PubSub,
  channel: string,
  handler: MessageHandler
): Future[Subscription]
method unsubscribe*(bus: PubSub, subscription: Subscription): Future[void]
method publish*(
  bus: PubSub,
  channel, payload: string
): Future[int64]
method onStateChange*(bus: PubSub, handler: StateHandler)
method close*(bus: PubSub): Future[void]
```

The package umbrella re-exports only these public types, factories, methods,
and error types. Backend implementation types remain available from explicit
submodules for tests and advanced dependency injection, but are not required
for normal use.

### 4.1 Defaults and validation

- `connectTimeout`: 5 seconds.
- `operationTimeout`: 2 seconds.
- `memoryMaxEntries`: 4096.
- `pubSubMaxPendingMessages`: 1024 per subscription. When a handler is
  slower than delivery and its queue is full, new payloads are dropped for
  that subscription; publish acceptance and counts are unchanged.
- `onHandlerError`: nil/no-op. The library has no logging dependency.
- `ttlSeconds == 0`: no expiry.
- `ttlSeconds < 0`: `InvalidArgumentError`.
- Empty keys and channels are rejected.
- Empty values and payloads are valid.
- Empty URL is normalized to `mem://`.
- Unknown schemes fail during `open*`; never silently fall back to memory.

### 4.2 Error model

All public failures derive from `RedisChronosError`:

- `InvalidArgumentError`
- `InvalidBackendUrlError`
- `BackendClosedError`
- `BackendTimeoutError`
- `BackendConnectionError`
- `RedisAuthenticationError`
- `RedisCommandError`
- `ProtocolError`

Chronos cancellation remains cancellation and is never wrapped. Error messages
must redact credentials and command payloads.

## 5. Portable behavioral contract

### 5.1 KV

- Missing `get` returns `none(string)`.
- `set` followed by `get` returns the exact bytes stored.
- `set` overwrites an existing value.
- `delete` returns true only when a key existed.
- `exists` agrees with `get`.
- TTL is not observable after expiry; early eviction is permitted only when
  the configured capacity is exceeded.
- `increment` treats a missing key as zero, stores the result, and returns it.
- `increment` on a non-integer value returns a typed command error and leaves
  the value unchanged.
- Interleaved increments on one event loop are atomic.
- Operations after `close` return `BackendClosedError`.
- Repeated `close` succeeds.

### 5.2 Pub/Sub

- Each active subscription receives messages for its exact channel only.
- Multiple subscriptions to the same channel each receive the message once.
- A successful publish returns the number of active local subscriptions on
  that `PubSub` instance for the exact channel. External Redis clients do not
  affect this backend-neutral result. Publish with no local subscribers
  succeeds and returns zero.
- `unsubscribe` stops future delivery and is idempotent.
- Delivery is at-most-once and ephemeral; history is never retained.
- Order is preserved per publisher connection and subscription.
- Publish completion means the backend accepted the publish, not that every
  asynchronous handler completed.
- Handler failure is isolated from other subscriptions and does not terminate
  the subscriber loop. It is reported to `onHandlerError` without including
  the message payload.
- Reconnection may lose messages. State hooks allow consumers to resynchronize.
- Registering `onStateChange` immediately reports the current state, then each
  later transition, so a handler registered after `openPubSub` cannot miss the
  initial connected state.
- Repeated `close` succeeds and terminates subscriptions and background tasks.

### 5.3 Concurrency

Backend objects belong to one Chronos event loop. Calls may interleave at
`await` points on that loop. Cross-thread use is unsupported. Separate Redis
backend objects may be created per thread because Redis is the shared state.
Memory backends are process-local and do not provide cross-process coherence.

## 6. Phase 1: memory backends

### 6.1 InMemoryKvStore

Use a table from key to:

```nim
Entry = object
  value: string
  expiresAt: Moment  # zero means no expiry
  lastAccess: uint64
```

Maintain a monotonic access counter. On get/set/increment, remove expired
entries encountered. Before capacity eviction, purge all expired entries,
then evict the least-recently-used entry with a deterministic key tie-break.
No background sweeper is required: lazy expiry plus bounded writes keeps the
implementation small and memory bounded.

Never expose table references or mutable entry storage. Snapshot what is
needed before an `await`.

### 6.2 InProcessPubSub

Maintain channel-to-subscription lists and unique subscription IDs.
Publishing snapshots the matching subscriptions before invoking handlers so a
handler may subscribe or unsubscribe without corrupting iteration.

Handler futures run independently. Capture failures, keep other handlers
alive, and report `HandlerError` through `BackendOptions.onHandlerError`.
Never include the payload in the error event. Do not add logging dependencies
to the core.

`onStateChange` reports `csConnected` after open and `csClosed` during close.
Memory never emits disconnected/reconnected states.

### 6.3 Phase 1 release gate

Release `v0.1.0` only when:

- Every portable contract test passes against memory under refc and ORC.
- Capacity and TTL tests are deterministic.
- No Redis package or running service is needed.
- The README contains a complete memory usage example.
- Public API review confirms Phase 2 needs no signature changes.

## 7. Phase 2: Redis backends

### 7.1 RESP2 codec

Implement incremental encoding/decoding for:

- Simple strings
- Errors
- Integers
- Bulk strings, including nil and binary payloads
- Arrays, including nested values and nil arrays

The parser must tolerate arbitrary TCP fragmentation, multiple frames in one
read, and CRLF split across reads. Enforce configurable maximum bulk and array
sizes before allocation. Protocol violations close the connection and return
`ProtocolError`.

### 7.2 URL and handshake

Support:

```text
redis://host:port
redis://:password@host:port/db
redis://username:password@host:port/db
```

Percent-decode credentials, validate port and database, redact secrets in all
errors, and default to port 6379/database 0. On connect:

1. Establish a Chronos TCP stream within `connectTimeout`.
2. AUTH when credentials are present.
3. SELECT when database is nonzero.
4. PING to prove readiness.

### 7.3 Redis command connection

Use one connection and serialize commands with a Chronos-compatible async
lock. One outstanding command keeps response correlation trivial. Each
operation is bounded by `operationTimeout`.

If the connection fails:

- Fail the current operation with a typed connection/timeout error.
- Mark the connection disconnected and close its stream.
- Reconnect on the next operation.
- Do not automatically replay the failed operation, including reads. This
  uniform rule avoids hidden ambiguity and keeps behavior explainable.

Map KV methods to `GET`, `SET` with optional `EX`, `DEL`, `EXISTS`, and
`INCR`. Validate reply shapes and convert Redis error replies to typed errors.

### 7.4 Redis Pub/Sub connection

Use a dedicated subscriber connection because a RESP2 subscribed connection
cannot run ordinary commands. A `RedisPubSub` therefore owns:

1. One serialized command connection used only for `PUBLISH`.
2. One dedicated subscriber connection used for subscribe/unsubscribe and the
   reader loop.

- Maintain desired subscriptions separately from live server state.
- Run one reader loop.
- On disconnect, emit `csDisconnected`, reconnect with capped exponential
  backoff plus jitter, authenticate/select, resubscribe, then emit
  `csConnected`.
- Closing cancels reconnect, closes the stream, resolves pending lifecycle
  futures, and emits `csClosed`.
- Subscription changes during reconnect update desired state and are applied
  after connection.
- Never claim delivery for messages lost while disconnected.

### 7.5 Phase 2 release gate

Release the first stable Redis-ready version only when:

- The same portable contract suite passes unchanged against memory and a real
  Redis 7 service under refc and ORC.
- Parser fragmentation, AUTH, SELECT, timeout, disconnect, reconnect,
  resubscribe, cancellation, and close tests pass.
- Stopping Redis cannot hang any operation beyond its configured deadline.
- Restarting Redis restores subscriptions and emits state transitions.
- No credentials or payloads appear in captured logs/errors.
- Memory examples still work unchanged.
- Redis examples differ only by URL and optional timeout configuration.

After this gate, production activation is operational only: deploy Redis,
provide credentials, and set the URL.

## 8. Package layout

```text
src/
  redischronos.nim
  redischronos/
    api.nim
    errors.nim
    options.nim
    memorykv.nim
    memorypubsub.nim
    resp2.nim
    redisurl.nim
    redisconnection.nim
    rediskv.nim
    redispubsub.nim
tests/
  tall.nim
  contract/
    kvcontract.nim
    pubsubcontract.nim
  tmemorykv.nim
  tmemorypubsub.nim
  tresp2.nim
  tredisurl.nim
  trediskv.nim
  tredispubsub.nim
```

Create modules only when their TODO becomes active; the layout is a target,
not permission to add empty files.

## 9. Test strategy

### 9.1 Contract harness

Contract tests accept async factories for `KvStore` and `PubSub`. Run the same
suite for:

- `mem://` unconditionally.
- `REDIS_TEST_URL` when provided.

Tests generate a unique key/channel prefix and clean it up. No test depends on
ordering with another test.

### 9.2 Protocol units

Use table-driven byte sequences and feed every frame:

- as one buffer;
- byte-by-byte;
- split at every possible boundary;
- concatenated with the next frame.

Include nil versus empty bulk strings, UTF-8 and binary bytes, negative
integers, server errors, malformed lengths, excessive lengths, truncated
frames, nested arrays, and pub/sub message arrays.

### 9.3 Network integration

CI uses Redis 7 with a health check. Dedicated scenarios start isolated Redis
instances or use a controllable proxy for:

- password and ACL authentication;
- database selection;
- delayed replies and operation timeout;
- connection termination during a command;
- subscriber restart and resubscription.

Every wait has a deadline. Tests must never sleep indefinitely.

### 9.4 CI matrix

- Phase 1: memory suite on Nim 2.2.x × refc/ORC.
- Phase 2: full suite on Nim 2.2.x × refc/ORC with Redis 7.
- Lint/format checks may be added only after a formatter configuration is
  committed and does not rewrite generated dependencies.

## 10. Deployment-readiness invariant

Before any application relies on the configuration-only swap, it pins a
released version that already contains and tests both backends. The application
must exercise its own behavior once with `mem://` and once with a real
`redis://` URL, but it must not branch on backend type.

The operational switch is then:

1. Deploy a private Redis 7 instance.
2. Configure authentication, memory limit, and an eviction policy appropriate
   to the workload.
3. Set the application URL.
4. Restart and verify health.

No change to this library is part of those deployment steps.
