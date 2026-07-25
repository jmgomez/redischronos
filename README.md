# redischronos

A small Chronos-native key/value and Pub/Sub library for Nim with interchangeable
in-memory and Redis backends.

The project is currently design-scaffolded. Implementation is tracked in
`TODO.md` and specified in `docs/design.md`.

The intended final usage is:

```nim
let store = await openKvStore(getEnv("REDIS_URL", "mem://"))
let bus = await openPubSub(getEnv("REDIS_URL", "mem://"))
defer:
  await bus.close()
  await store.close()
```

An empty URL or `mem://` selects process-local memory. A `redis://` URL selects
Redis. The public API and application call sites remain unchanged.

