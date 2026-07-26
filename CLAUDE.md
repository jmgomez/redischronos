# redischronos — Chronos-native KV and Pub/Sub

`redischronos` provides a small backend-neutral `KvStore` and `PubSub` API
for Nim applications using Chronos. It ships process-local memory backends
first and Redis backends second. Both implementations must pass the same
behavioral contract suite.

The completed package selects its backend by URL:

- empty string or `mem://` — in-memory KV and in-process Pub/Sub
- `redis://...` — Redis KV and Redis Pub/Sub

Consumers must not change imports or call sites when switching backends.

## Required reading

- `docs/design.md` — authoritative API, semantics, architecture, failure
  behavior, and acceptance criteria.
- `TODO.md` — ordered TDD implementation plan. Work from top to bottom unless
  the user changes priority.

## Architecture boundaries

- The library owns mechanisms: key/value operations, TTL, bounded in-memory
  storage, atomic counters, pub/sub transport, RESP2, deadlines, connection
  lifecycle, and the opt-in generic cache layered on an injected `KvStore`.
- Consumers own policies: domain key naming, serialization, when and what to
  invalidate, cache failure/cancellation option selection, and domain event
  schemas.
- Keep the public API small. Do not add domain cache frameworks, ORM
  integrations, distributed locks, queues, or application-specific helpers.
- The in-memory backend is local to one process and one Chronos event loop.
  Cross-thread use is out of contract.
- `RedisKvStore` owns one serialized command connection. `RedisPubSub` owns
  one serialized publish-command connection plus one dedicated subscriber
  connection. Do not add pooling or pipelining without evidence that the
  simple design is insufficient.

## TDD workflow

Every TODO item follows red/green/refactor:

1. Write one focused behavioral test from the acceptance criteria.
2. Run it and confirm it fails for the intended reason.
3. Review the test for false positives.
4. Implement the minimum behavior.
5. Run the focused test.
6. Run the full contract suite under refc and ORC.
7. Refactor only while green.

Do not mark a TODO checkbox complete until its acceptance tests pass. For
Redis work, both the memory and real-Redis variants must pass.

## Commands

Nim is not assumed to be on `PATH`. Set `NIM_BIN` when necessary:

```bash
export NIM_BIN=/Users/jmgomez/.nimble/nimbinaries/nim-2.2.10/bin/nim
nimble test
nimble testRefc
nimble testOrc
nimble testFile tests/tname.nim
```

## Dependencies

- Nim >= 2.2
- Chronos >= 4.0.0 and < 5.0.0
- No asyncdispatch dependency, directly or transitively
- No system Redis client library: RESP2 is implemented over Chronos streams
- Must compile and pass tests under refc and ORC

## Conventions

### Nim

- **lowercase** for file names (`connection.nim`, not `Connection.nim`)
- **camelCase** for properties, fields, variables, and procs
- Use Chronos for all asynchronous I/O and timing
- Keep exceptions typed at the library boundary
- Bound all external I/O with a deadline
- Make `close` idempotent and cancellation-safe

### General

- Prefer editing existing files over creating unnecessary abstractions.
- Keep solutions simple and directly tied to the specification.
- Umbrella module `src/redischronos.nim` only re-exports the public API.
- Never log credentials, full Redis URLs, or command payloads.

## Testing requirements

- Always run tests before reporting completion.
- Every public behavior is tested through the backend contract suite.
- Protocol parsing gets table-driven unit tests for fragmented input.
- Network tests use a real Redis service, bounded waits, and deterministic
  cleanup.
- CI runs refc and ORC. Once Phase 2 starts, CI also runs the contract suite
  against real Redis.

## Documentation

- `docs/design.md` is the source of truth. Change it before changing a public
  contract.
- Keep README, TODO acceptance criteria, and implementation synchronized.
- Record newly discovered protocol or lifecycle constraints immediately.

## TODO.md task queue

This project uses `TODO.md` as an async task queue. At the start of each
session, inspect pending items through Meta and work through them in order
unless the user gives another priority. Mark items done only after their
acceptance criteria pass.

## Git workflow

- Private repository; `devel` is the working branch.
- Use conventional commits: `feat:`, `fix:`, `test:`, `docs:`, `refactor:`.
- All tests pass before push.
- Releases use annotated semantic-version tags.
- Never force-push a published branch or tag.
