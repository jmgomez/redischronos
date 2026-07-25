# redischronos implementation queue

The order is deliberate. Every implementation task starts with a failing test
and is complete only after its acceptance criteria pass. Do not check deferred
items merely because their design is complete.

## Foundation

- [x] Scaffold the package, TDD harness, project instructions, CI memory
  matrix, authoritative design, and implementation queue. Acceptance:
  `nimble testRefc` and `nimble testOrc` pass on the public smoke test.

## Phase 1 — memory-only release (`v0.1.0`)

- [x] **P1.1 — Freeze the public contracts and errors test-first.** RED:
  add compile-time and runtime tests for `BackendOptions`, typed errors,
  handler-error observer, abstract `KvStore`/`PubSub`, subscription identity,
  factories, and idempotent lifecycle. GREEN: implement only `api.nim`,
  `options.nim`, and `errors.nim`. ACCEPT: the documented public example
  compiles; unknown URL schemes fail explicitly; empty URL and `mem://` choose
  memory; no Redis modules are needed.

- [x] **P1.2 — Implement basic in-memory KV round-trips.** RED: parameterized
  contract tests for missing get, binary-safe set/get, overwrite, delete
  boolean, exists agreement, empty value, and invalid empty keys. GREEN:
  implement the smallest `InMemoryKvStore` table and wire `openKvStore`.
  ACCEPT: focused tests and the full refc/ORC suite pass with no network.

- [x] **P1.3 — Add TTL behavior.** RED: tests prove no-expiry entries remain,
  positive-TTL entries disappear, negative TTL is rejected, overwrite resets
  TTL, and expired entries are never returned by exists/increment. Prefer an
  injected clock or deterministic test seam over sleeps. GREEN: add lazy
  expiry. ACCEPT: tests are deterministic under repeated runs and both memory
  managers.

- [x] **P1.4 — Add bounded deterministic LRU eviction.** RED: tests cover
  configured capacity, read-refreshes-recency, overwrite behavior, expired
  entries purged before live eviction, and deterministic tie-breaking. GREEN:
  add the access counter and eviction path without a background sweeper.
  ACCEPT: entry count never exceeds `memoryMaxEntries`; capacity zero/negative
  configuration is rejected; contract still permits early loss under pressure.

- [x] **P1.5 — Add atomic increment semantics.** RED: tests cover missing→1,
  monotonic increments, negative stored integers, overflow, non-integer
  preservation, and many interleaved increments on one Chronos loop. GREEN:
  implement parse/check/store without an await between read and write. ACCEPT:
  no lost updates and Redis-compatible error behavior.

- [x] **P1.6 — Implement in-process Pub/Sub.** RED: tests cover exact-channel
  isolation, multiple subscribers, publish count, no-subscriber publish,
  idempotent unsubscribe, subscribe/unsubscribe inside a callback, ordered
  delivery, one failed handler not killing others, and redacted handler-error
  observation. GREEN: implement snapshot-based delivery and unique
  subscription handles. ACCEPT: no retained history, no iterator corruption,
  payloads never enter error events, and all tasks are drained on close.

- [x] **P1.7 — Complete lifecycle and state semantics.** RED: tests cover
  connected/closed notifications, operations after close, double close,
  close with active subscriptions, and cancellation during handler execution.
  GREEN: implement deterministic shutdown. ACCEPT: no leaked Chronos futures or
  tasks under the test leak checks; memory never reports a fake disconnect.

- [x] **P1.8 — Extract and publish the reusable backend contract suite.** RED:
  prove a deliberately broken fake backend fails each important contract.
  GREEN: move portable KV/PubSub behavior into async factory-driven test
  helpers. ACCEPT: memory is tested only through the same harness Phase 2 will
  use; backend-specific tests cover only LRU and memory lifecycle internals.

- [ ] **P1.9 — Documentation, examples, and memory release gate.** Write
  README examples for KV, TTL, Pub/Sub, error handling, and shutdown; document
  process/event-loop boundaries. Run the complete suite repeatedly under refc
  and ORC, review the public API against Phase 2, create the annotated `v0.1.0`
  tag, and publish only if no Phase 2 signature change is foreseen. ACCEPT:
  fresh-clone install/build/test succeeds without Redis.

## Phase 2 — Redis-ready release

- [ ] **P2.1 — Implement the RESP2 value model and encoder.** RED: table tests
  for simple strings, errors, integers, binary/nil bulk strings, arrays,
  nested arrays, and command argument framing. GREEN: implement `resp2.nim`
  encoding only. ACCEPT: byte-for-byte fixtures match Redis protocol and
  command values cannot inject framing.

- [ ] **P2.2 — Implement the incremental RESP2 parser.** RED: feed every
  fixture whole, byte-by-byte, split at every boundary, and concatenated;
  include CRLF splits, nil/empty distinction, malformed and oversized lengths,
  truncation, nesting limits, and multiple frames. GREEN: implement a bounded
  stateful parser. ACCEPT: incomplete data requests more bytes; invalid data
  returns `ProtocolError`; allocation limits are enforced before allocation.

- [ ] **P2.3 — Parse and redact Redis URLs.** RED: cover default port/database,
  password auth, ACL username/password, percent encoding, IPv4/IPv6, invalid
  ports/databases/schemes, absent passwords, and redacted errors. GREEN:
  implement `redisurl.nim`. ACCEPT: no formatted/debug representation exposes
  credentials; `rediss://` fails clearly as unsupported.

- [ ] **P2.4 — Build the serialized command connection.** RED: scripted-server
  tests cover connect deadline, AUTH variants, SELECT, PING, one-command
  correlation, server errors, malformed replies, cancellation, operation
  timeout, close, and disconnect mid-command. GREEN: implement one Chronos
  stream guarded by an async lock. ACCEPT: one outstanding command, all I/O
  bounded, current ambiguous operations never replayed, next operation may
  reconnect.

- [ ] **P2.5 — Implement `RedisKvStore` through the shared contract.** RED:
  run the unchanged KV contract against Redis and add mapping tests for GET,
  SET/EX, DEL, EXISTS, INCR, nil, and wrong-type/non-integer errors. GREEN:
  implement only the command mappings and factory selection. ACCEPT: memory
  and Redis variants pass the same suite; choosing Redis requires only a URL.

- [ ] **P2.6 — Build the Redis subscriber connection.** RED: scripted and
  real-Redis tests cover the separate publish-command and subscriber
  connections, subscribe acknowledgements, message arrays, exact channel
  routing, multiple local handlers sharing a server subscription, unsubscribe
  acknowledgements, and publish counts. GREEN: implement one reader loop plus
  desired/live subscription state. ACCEPT: `PUBLISH` uses the serialized
  publish connection, other command traffic never uses the subscribed
  connection, and handler failures remain isolated.

- [ ] **P2.7 — Add subscriber reconnect and resubscription.** RED: terminate
  Redis during active subscriptions; assert bounded disconnected state,
  capped exponential backoff with jitter, subscription changes while offline,
  automatic resubscription after restart, connected state, and clean close
  during backoff. GREEN: implement the reconnect state machine. ACCEPT:
  messages during downtime are explicitly not promised; no duplicate live
  subscriptions or orphan reconnect tasks remain.

- [ ] **P2.8 — Harden error, cancellation, and secret handling.** RED:
  fault-injection tests cover timeouts at every handshake/command stage,
  cancellation while waiting for the lock/read/reconnect, protocol size
  attacks, handler failures, and captured diagnostic output containing
  credentials/payloads. GREEN: close lifecycle gaps without broad retries.
  ACCEPT: no hang exceeds its deadline, cancellation is never wrapped, and
  secrets/payloads are absent from errors and logs.

- [ ] **P2.9 — Run the full Redis CI matrix.** Add a healthy Redis 7 service
  and execute the unchanged contract suite on Nim 2.2.x × refc/ORC. Add
  bounded AUTH/database and restart scenarios. ACCEPT: memory remains green;
  real Redis is mandatory rather than silently skipped in the Redis job; all
  network resources are cleaned up.

- [ ] **P2.10 — Prove configuration-only backend switching.** Build one
  backend-neutral example/test program and run it unchanged first with
  `mem://`, then with `REDIS_TEST_URL`; compare contract-observable results.
  ACCEPT: no backend type checks, conditional imports, or changed call sites;
  only the URL differs.

- [ ] **P2.11 — Stable release and deployment handoff.** Complete API review,
  README Redis examples, operational timeout/reconnect guidance, compatibility
  table, and changelog. Run refc/ORC memory+Redis suites from a clean clone,
  create an annotated stable tag, and record the commit. ACCEPT: the pinned
  release already contains both backends, so later activation consists only
  of deploying Redis and setting the URL—no library changes.
