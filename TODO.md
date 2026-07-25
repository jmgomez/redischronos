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

- [x] **P1.9 — Documentation, examples, and memory release gate.** Write
  README examples for KV, TTL, Pub/Sub, error handling, and shutdown; document
  process/event-loop boundaries. Run the complete suite repeatedly under refc
  and ORC, review the public API against Phase 2, create the annotated `v0.1.0`
  tag, and publish only if no Phase 2 signature change is foreseen. ACCEPT:
  fresh-clone install/build/test succeeds without Redis.

## Phase 2 — Redis-ready release

- [x] **P2.1 — Implement the RESP2 value model and encoder.** RED: table tests
  for simple strings, errors, integers, binary/nil bulk strings, arrays,
  nested arrays, and command argument framing. GREEN: implement `resp2.nim`
  encoding only. ACCEPT: byte-for-byte fixtures match Redis protocol and
  command values cannot inject framing.

- [x] **P2.2 — Implement the incremental RESP2 parser.** RED: feed every
  fixture whole, byte-by-byte, split at every boundary, and concatenated;
  include CRLF splits, nil/empty distinction, malformed and oversized lengths,
  truncation, nesting limits, and multiple frames. GREEN: implement a bounded
  stateful parser. ACCEPT: incomplete data requests more bytes; invalid data
  returns `ProtocolError`; allocation limits are enforced before allocation.

- [x] **P2.3 — Parse and redact Redis URLs.** RED: cover default port/database,
  password auth, ACL username/password, percent encoding, IPv4/IPv6, invalid
  ports/databases/schemes, absent passwords, and redacted errors. GREEN:
  implement `redisurl.nim`. ACCEPT: no formatted/debug representation exposes
  credentials; `rediss://` fails clearly as unsupported.

- [x] **P2.4 — Build the serialized command connection.** RED: scripted-server
  tests cover connect deadline, AUTH variants, SELECT, PING, one-command
  correlation, server errors, malformed replies, cancellation, operation
  timeout, close, and disconnect mid-command. GREEN: implement one Chronos
  stream guarded by an async lock. ACCEPT: one outstanding command, all I/O
  bounded, current ambiguous operations never replayed, next operation may
  reconnect.

- [x] **P2.5 — Implement `RedisKvStore` through the shared contract.** RED:
  run the unchanged KV contract against Redis and add mapping tests for GET,
  SET/EX, DEL, EXISTS, INCR, nil, and wrong-type/non-integer errors. GREEN:
  implement only the command mappings and factory selection. ACCEPT: memory
  and Redis variants pass the same suite; choosing Redis requires only a URL.

- [x] **P2.6 — Build the Redis subscriber connection.** RED: scripted and
  real-Redis tests cover the separate publish-command and subscriber
  connections, subscribe acknowledgements, message arrays, exact channel
  routing, multiple local handlers sharing a server subscription, unsubscribe
  acknowledgements, and publish counts. GREEN: implement one reader loop plus
  desired/live subscription state. ACCEPT: `PUBLISH` uses the serialized
  publish connection, other command traffic never uses the subscribed
  connection, and handler failures remain isolated.

- [x] **P2.7 — Add subscriber reconnect and resubscription.** RED: terminate
  Redis during active subscriptions; assert bounded disconnected state,
  capped exponential backoff with jitter, subscription changes while offline,
  automatic resubscription after restart, connected state, and clean close
  during backoff. GREEN: implement the reconnect state machine. ACCEPT:
  messages during downtime are explicitly not promised; no duplicate live
  subscriptions or orphan reconnect tasks remain.

- [x] **P2.8 — Harden error, cancellation, and secret handling.** RED:
  fault-injection tests cover timeouts at every handshake/command stage,
  cancellation while waiting for the lock/read/reconnect, protocol size
  attacks, handler failures, and captured diagnostic output containing
  credentials/payloads. GREEN: close lifecycle gaps without broad retries.
  ACCEPT: no hang exceeds its deadline, cancellation is never wrapped, and
  secrets/payloads are absent from errors and logs.

- [x] **P2.9 — Run the full Redis CI matrix.** Add a healthy Redis 7 service
  and execute the unchanged contract suite on Nim 2.2.x × refc/ORC. Add
  bounded AUTH/database and restart scenarios. ACCEPT: memory remains green;
  real Redis is mandatory rather than silently skipped in the Redis job; all
  network resources are cleaned up.

- [x] **P2.10 — Prove configuration-only backend switching.** Build one
  backend-neutral example/test program and run it unchanged first with
  `mem://`, then with `REDIS_TEST_URL`; compare contract-observable results.
  ACCEPT: no backend type checks, conditional imports, or changed call sites;
  only the URL differs.

- [x] **P2.11 — Stable release and deployment handoff.** Complete API review,
  README Redis examples, operational timeout/reconnect guidance, compatibility
  table, and changelog. Run refc/ORC memory+Redis suites from a clean clone,
  create an annotated stable tag, and record the commit. ACCEPT: the pinned
  release already contains both backends, so later activation consists only
  of deploying Redis and setting the URL—no library changes.
- [x] **AUDIT-C1 — Make RedisConnection close terminal under queued-command races (TDD).** RED: scripted server test holds command A, queues command B, calls close, then releases A; assert close is bounded, B fails BackendClosedError, and no post-close TCP reconnect/command occurs. GREEN: serialize close/admission with the command lock or lifecycle generation and re-check closing/closed after lock acquisition and before establish/send. ACCEPT: queued and new operations cannot execute once close begins; repeated close remains idempotent under refc/ORC.
- [x] **AUDIT-C2 — Preserve TTL across increment in both backends (TDD).** RED: extend the unchanged KV contract with set(counter, 1, ttl) → increment → advance/wait past expiry; prove memory currently persists while Redis expires. GREEN: retain expiresAt when memory increment replaces an existing Entry. ACCEPT: injected-clock memory and real-Redis refc/ORC tests agree, including invalid/overflow values leaving TTL and value unchanged.
- [x] **AUDIT-C3 — Freeze backend-neutral Pub/Sub publish result semantics (TDD).** RED: shared contract creates two local handlers on one channel and records the public publish result for mem:// and redis://; include zero, one, two local handlers and another Redis client. GREEN: choose and document one portable meaning (or explicitly only portable zero/nonzero), then implement it consistently without backend type branches at call sites. ACCEPT: the unchanged shared test and backend-switch program produce the declared semantics under both URLs.
- [x] **AUDIT-C4 — Make every reconnect attempt resource-safe (TDD).** RED: scripted subscriber completes handshake then fails/malforms/stalls resubscribe for several attempts; assert every rejected socket observes EOF and transport/client counts stay bounded. GREEN: keep candidate transport/parser local until full resubscribe succeeds and closeWait it on every failure/cancellation before retry. ACCEPT: no descriptor, Redis-client, parser, or reconnect-task leak across repeated faults and close.
- [x] **AUDIT-C5 — Put one real deadline around subscriber lock/write/ACK I/O (TDD).** RED: fault-inject lock wait, peer-not-reading write, missing SUBSCRIBE/UNSUBSCRIBE ACK, cancellation, and close while lifecycle futures are pending. GREEN: propagate one absolute remaining deadline across lock, write, and ACK read; close the ambiguous attempt and resolve/fail pending futures on timeout/close. ACCEPT: no subscriber operation or reconnect stage exceeds the configured bound and cancellation remains CancelledError.
- [x] **AUDIT-C6 — Prove live recovery through a sustained real Redis outage in CI (TDD).** RED: with an active subscription stop Redis, observe bounded csDisconnected, keep it down across multiple backoffs, add/remove desired subscriptions offline, restart, then require csConnected and exactly one delivery per active handler; also close while Redis remains down. GREEN: fix only state-machine defects exposed by this scenario. ACCEPT: the refc/ORC CI matrix drives the existing bus through the outage; restarting Redis before a fresh suite is not the proof.
- [x] **AUDIT-C7 — Make RESP inline parsing invariant to TCP fragmentation (TDD).** RED: feed valid long simple/error/integer/length lines whole, byte-by-byte, and at every split; include a standard long Redis error and explicit over-limit cases. GREEN: add a documented/configurable inline-line limit and enforce it identically before and after CRLF discovery. ACCEPT: every valid fixture has identical results for every boundary, while over-limit data fails before unbounded buffering.
- [x] **AUDIT-C8 — Reconcile desired/live subscriptions safely during reconnect (TDD).** RED: barrier-control a stalled resubscribe ACK while adding/removing channels; assert no Table iterator defect, duplicate, or orphan server subscription and correct publish counts after recovery. GREEN: snapshot desired state or serialize mutations, reconcile additions and removals until desired == live, and keep one authoritative reader path. ACCEPT: offline changes at every reconnect stage converge under refc/ORC.
- [x] **AUDIT-W1 — Preserve active commands when a queued lock waiter is cancelled or times out (TDD).** RED: command A owns the lock while waiter B is cancelled and waiter C times out; assert A still completes and the connection remains usable. GREEN: cancel only the acquire future before ownership; disconnect only when the cancelled operation owned the lock and may have performed I/O. ACCEPT: waiter cancellation never makes another command ambiguous.
- [x] **AUDIT-W2 — Make subscribe/unsubscribe acknowledgement state transactional (TDD).** RED: cover pre-write failure, ambiguous timeout, disconnect, late ACK, retry on the same channel, and close with pending subscribe/unsubscribe. GREEN: define rollback/handle semantics, remove or cancel stale ACK entries, deactivate unreachable ghost handlers, and fail all pending lifecycle futures on close. ACCEPT: a raised subscribe cannot later create an unowned live handler and no ACK future/table entry leaks.
- [x] **AUDIT-W3 — Isolate state observers from transport progress and shutdown (TDD).** RED: register a never-finishing and a repeatedly replaced StateHandler; disconnect/reconnect and close both memory and Redis buses under deadlines. GREEN: serialize notifications in one owned observer queue/worker and cancel/drain it deterministically without blocking reconnect. ACCEPT: transitions remain ordered, handler failures are isolated, and no callback task survives close.
- [x] **AUDIT-W4 — Bound and compact Pub/Sub delivery state (TDD).** RED: block a handler, flood publishes, churn subscriptions, and unsubscribe with queued payloads; assert a documented queue bound/overflow outcome, no callback starts after unsubscribe resolves, and inactive/channel counts return to baseline. GREEN: use a deque/ring buffer, enforce an explicit overflow/backpressure policy, re-check active before callbacks, and remove inactive handles/empty channels. ACCEPT: memory use and per-message dequeue cost stay bounded in both backends.
- [x] **AUDIT-W5 — Reject surplus command replies and preserve all subscriber frames (TDD).** RED: scripted command reply sends two RESP frames and reconnect sends SUBSCRIBE ACK plus message in one TCP read. GREEN: treat surplus non-pipelined command replies as ProtocolError/close, while routing every parsed subscriber frame through the reader/ACK dispatcher instead of readOne dropping extras. ACCEPT: no command desynchronization and no coalesced Pub/Sub message loss.
- [x] **AUDIT-W6 — Define end-to-end command/connect deadline semantics and typed DNS failures (TDD).** RED: stack lock, resolve/connect, handshake, and reply delays and assert the documented total elapsed bound; inject resolution failure. GREEN: compute absolute deadlines and pass remaining budgets through stages, resolve off-loop/under deadline, try addresses as specified, and map resolver failures to BackendConnectionError. ACCEPT: connectTimeout and operationTimeout are not silently truncated or multiplied.
- [x] **AUDIT-W7 — Seed reconnect jitter per instance (TDD).** RED: an injected jitter source proves range/cap behavior and that independent production instances do not share an unseeded deterministic sequence. GREEN: use system entropy for a per-instance RNG while retaining deterministic injection for tests. ACCEPT: backoff remains capped, cancellable, and fleet-wide reconnects are actually dispersed.
- [x] **AUDIT-W8 — Turn checked release claims into mandatory contract evidence (TDD/CI).** Expand broken KV and Pub/Sub contract fakes to fail every portable behavior; add unique test namespaces/finally cleanup and deadline-driven futures instead of fixed sleeps; strengthen secret checks to secret notin diagnostics; run real password-only and ACL Redis plus verified SELECT isolation; compile the backend-switch example once and diff the same binary's mem/Redis output. ACCEPT: refc/ORC CI fails for each deliberately broken behavior and cannot pass with skipped AUTH, SELECT, restart, redaction, or switching evidence.
