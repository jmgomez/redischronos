# Audit 4 implementation plan

1. In `tests/tcache.nim`, add deterministic tests for cancellation-resistant stale writes racing a same-key replacement and cache close, including compensating-delete failure. In `src/redischronos/cache.nim`, make physical stale-write retirement replacement-safe and make terminal cleanup failures observable according to `CacheFailurePolicy`.
2. In the memory and Redis Pub/Sub tests, add a yielding state handler that awaits `close()` for every state including `csClosed`, with concurrent external close. Update `src/redischronos/memorypubsub.nim` and `src/redischronos/redispubsub.nim` so terminal observer re-entry cannot join its own shutdown cycle and all caller/worker tracking drains.
3. In `tests/tredisresolve.nim`, replace the hard-coded resolver worker assertion with stress coverage that observes actual worker creation and liveness. Add minimal thread-safe instrumentation at the production worker boundary in `src/redischronos/redisresolve.nim`.
4. Run focused tests for cache, memory/Redis Pub/Sub, and resolver under refc and ORC. Then run the full memory and live-Redis refc/ORC suites. Update `docs/design.md` for any newly frozen cleanup or lifecycle semantics.

Do not modify public API contracts beyond behavior explicitly required by the three Audit 4 TODOs. Preserve the user-authored `TODO.md` items and do not mark them complete.
