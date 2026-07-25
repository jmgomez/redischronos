# Changelog

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
