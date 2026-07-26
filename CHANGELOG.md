# Changelog

## 1.1.2 — 2026-07-26

- Make cache shutdown translate internal cancellation, detect yielded
  recursion, hand off loader-initiated close safely, and compensate backend
  writes that cross a close fence.
- Make callback-initiated Pub/Sub close follow arbitrary async helper ancestry
  while pruning all completed caller tracking.
- Replace deprecated unbounded threadpool DNS work with one bounded resolver
  worker and bounded request/result queues per event-loop thread.
- Expand lifecycle, failure-policy, resolver stress, cleanup, and release
  evidence.

## 1.1.1 — 2026-07-25

- Fence cache fills against invalidation, version changes, orphan replacement,
  and close while owning blocked reads and waiters.
- Make message and state callbacks safe to await Pub/Sub close.
- Close ambiguous subscriber writes on cancellation and compact retired
  delivery state during normal operation.
- Enforce one end-to-end command/connection deadline, return DNS results
  without a second event-loop lookup, and fix Redis ACL channel permissions.
- Strengthen cache failure-policy, mutation-oracle, and release evidence.

## 1.1.0 — 2026-07-25

- Make Redis command close/admission races terminal and preserve active
  commands when queued waiters are cancelled.
- Preserve memory TTL across increments and define portable Pub/Sub publish
  counts as active local subscriptions.
- Harden subscriber deadlines, transactional acknowledgements, candidate
  cleanup, desired/live reconciliation, observer isolation, bounded delivery,
  and per-instance reconnect jitter.
- Make RESP inline limits fragmentation-invariant, reject surplus command
  replies, and enforce end-to-end command deadlines with typed DNS failures.
- Add sustained real-Redis outage, password/ACL, and stronger broken-backend
  contract evidence to the refc/ORC matrix.
- Add an opt-in backend-neutral cache with coalesced fills, configurable
  cancellation/failure policy, TTL, invalidation, and version helpers.

## 1.0.0 — 2026-07-25

- Add interchangeable in-memory and Redis KV backends with TTL and atomic
  counters.
- Add interchangeable in-process and Redis Pub/Sub backends with isolated
  handlers, connection-state notifications, and automatic resubscription.
- Add bounded RESP2 parsing, serialized deadline-bound Redis connections,
  credential redaction, cancellation-safe lifecycle, and refc/ORC contract
  coverage against Redis 7.

## 0.1.0 — 2026-07-25

- Add the backend-neutral public API and process-local memory KV and Pub/Sub
  implementations.
