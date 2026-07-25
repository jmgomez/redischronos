# Changelog

## Unreleased

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
