import std/[options, os, tables, times, unittest]
import chronos

import redischronos
import redischronos/cache

type
  StoreFactory = proc(): Future[KvStore] {.gcsafe.}
  FailingStore = ref object of KvStore
  WriteFailStore = ref object of KvStore
  BlockingReadStore = ref object of KvStore
    started: Future[void]
    release: Future[void]
  LateWriteStore = ref object of KvStore
    values: Table[string, string]
    writeStarted: Future[void]
    writeRelease: Future[void]
  CancelledStore = ref object of KvStore
  CancelledWriteStore = ref object of KvStore

method get(store: FailingStore,
    key: string): Future[Option[string]] {.async.} =
  raise newException(BackendConnectionError, "injected read failure")

method set(store: FailingStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  raise newException(BackendConnectionError, "injected write failure")

method delete(store: FailingStore, key: string): Future[bool] {.async.} =
  raise newException(BackendConnectionError, "injected delete failure")

method increment(store: FailingStore, key: string): Future[int64] {.async.} =
  raise newException(BackendTimeoutError, "injected increment timeout")

method get(store: WriteFailStore,
    key: string): Future[Option[string]] {.async.} =
  return none(string)

method set(store: WriteFailStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  raise newException(BackendTimeoutError, "injected fill timeout")

method get(store: BlockingReadStore,
    key: string): Future[Option[string]] {.async.} =
  store.started.complete()
  await store.release.noCancel()
  return none(string)

method get(store: LateWriteStore,
    key: string): Future[Option[string]] {.async.} =
  if store.values.hasKey(key):
    return some(store.values[key])
  return none(string)

method set(store: LateWriteStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  store.writeStarted.complete()
  await store.writeRelease.noCancel()
  store.values[key] = value

method delete(store: LateWriteStore, key: string): Future[bool] {.async.} =
  result = store.values.hasKey(key)
  store.values.del(key)

method increment(store: LateWriteStore, key: string): Future[int64] {.async.} =
  return 1

method get(store: CancelledStore,
    key: string): Future[Option[string]] {.async.} =
  raise newException(CancelledError, "injected read cancellation")

method set(store: CancelledStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  raise newException(CancelledError, "injected write cancellation")

method delete(store: CancelledStore, key: string): Future[bool] {.async.} =
  raise newException(CancelledError, "injected delete cancellation")

method increment(store: CancelledStore, key: string): Future[int64] {.async.} =
  raise newException(CancelledError, "injected increment cancellation")

method get(store: CancelledWriteStore,
    key: string): Future[Option[string]] {.async.} =
  return none(string)

method set(store: CancelledWriteStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  raise newException(CancelledError, "injected write cancellation")

proc runCacheContract(name: string, factory: StoreFactory) =
  let prefix =
    "redischronos:test:cache:" & $getCurrentProcessId() & ":" &
    $epochTime() & ":"
  suite name:
    test "miss fills, hit bypasses loader, and invalidation reloads":
      proc exercise() {.async.} =
        let store = await factory()
        let cache = newCache(store)
        var loads = 0
        let loader: CacheLoader =
          proc(key: string): Future[string] {.async.} =
            inc loads
            return "value-" & $loads
        let key = prefix & "basic"
        check (await cache.getOrLoad(key, loader)) == "value-1"
        check (await cache.getOrLoad(key, loader)) == "value-1"
        check loads == 1
        check await cache.invalidate(key)
        check (await cache.getOrLoad(key, loader)) == "value-2"
        discard await cache.invalidate(key)
        await cache.close()
        await store.close()
      waitFor exercise()

    test "TTL expires a completed fill":
      proc exercise() {.async.} =
        let store = await factory()
        var options = defaultCacheOptions()
        options.ttlSeconds = 1
        let cache = newCache(store, options)
        var loads = 0
        let loader: CacheLoader =
          proc(key: string): Future[string] {.async.} =
            inc loads
            return $loads
        let key = prefix & "ttl"
        check (await cache.getOrLoad(key, loader)) == "1"
        await sleepAsync(1100)
        check (await cache.getOrLoad(key, loader)) == "2"
        discard await cache.invalidate(key)
        await cache.close()
        await store.close()
      waitFor exercise()

    test "same-key misses coalesce and one cancelled waiter is isolated":
      proc exercise() {.async.} =
        let store = await factory()
        let cache = newCache(store)
        let started = newFuture[void]("cache loader started")
        let release = newFuture[void]("cache loader release")
        var loads = 0
        let loader: CacheLoader =
          proc(key: string): Future[string] {.async.} =
            inc loads
            started.complete()
            await release
            return "shared"
        let cancelled = cache.getOrLoad(prefix & "shared", loader)
        await started
        let survivor = cache.getOrLoad(prefix & "shared", loader)
        cancelled.cancelSoon()
        expect CancelledError:
          discard await cancelled
        release.complete()
        check (await survivor) == "shared"
        check loads == 1
        discard await store.delete(prefix & "shared")
        await cache.close()
        await store.close()
      waitFor exercise()

    test "last-waiter cancellation policy and close drain owned fills":
      proc exercise() {.async.} =
        let store = await factory()
        var options = defaultCacheOptions()
        options.cancellationPolicy = ccpCancelOrphaned
        let cache = newCache(store, options)
        let started = newFuture[void]("orphan loader started")
        var terminated = false
        let loader: CacheLoader =
          proc(key: string): Future[string] {.async.} =
            if not started.finished:
              started.complete()
            try:
              await sleepAsync(chronos.hours(1))
              return "unreachable"
            finally:
              terminated = true
        let waiter = cache.getOrLoad(prefix & "orphan", loader)
        await started
        waiter.cancelSoon()
        expect CancelledError:
          discard await waiter
        await sleepAsync(10)
        check terminated

        let active = cache.getOrLoad(prefix & "close", loader)
        await sleepAsync(10)
        let first = cache.close()
        let second = cache.close()
        first.cancelSoon()
        await first
        await second.wait(chronos.milliseconds(100))
        expect BackendClosedError:
          discard await active
        expect BackendClosedError:
          discard await cache.getOrLoad(prefix & "closed", loader)
        await store.close()
      waitFor exercise()

    test "generic version helpers use atomic backend counters":
      proc exercise() {.async.} =
        let store = await factory()
        let cache = newCache(store)
        check versionedKey("profile", 3) == "profile:v3"
        let key = prefix & "version"
        check (await cache.bumpVersion(key)) == 1
        check (await cache.bumpVersion(key)) == 2
        discard await cache.invalidate(key)
        await cache.close()
        await store.close()
      waitFor exercise()

    test "invalidation fences an active fill":
      proc exercise() {.async.} =
        let store = await factory()
        let cache = newCache(store)
        let started = newFuture[void]("contract fenced loader started")
        let release = newFuture[void]("contract fenced loader release")
        let key = prefix & "fence"
        let loader: CacheLoader =
          proc(key: string): Future[string] {.async.} =
            started.complete()
            await release.noCancel()
            return "stale"
        let filling = cache.getOrLoad(key, loader)
        await started
        let invalidating = cache.invalidate(key)
        await sleepAsync(chronos.milliseconds(5))
        release.complete()
        discard await invalidating
        expect BackendClosedError:
          discard await filling
        check (await store.get(key)).isNone
        await cache.close()
        await store.close()
      waitFor exercise()

    test "version bump fences an active fill":
      proc exercise() {.async.} =
        let store = await factory()
        let cache = newCache(store)
        let started = newFuture[void]("versioned fill started")
        let release = newFuture[void]("versioned fill release")
        let key = prefix & "version-fence"
        let loader: CacheLoader =
          proc(key: string): Future[string] {.async.} =
            started.complete()
            await release.noCancel()
            return "stale"
        let filling = cache.getOrLoad(key, loader)
        await started
        let bumping = cache.bumpVersion(key)
        await sleepAsync(chronos.milliseconds(5))
        release.complete()
        check (await bumping) == 1
        expect BackendClosedError:
          discard await filling
        check (await store.get(key)) == some("1")
        discard await store.delete(key)
        await cache.close()
        await store.close()
      waitFor exercise()

runCacheContract(
  "memory cache contract",
  proc(): Future[KvStore] {.async.} =
    return await openKvStore("mem://")
)

when defined(redisIntegration):
  if getEnv("REDIS_TEST_URL").len == 0:
    quit "REDIS_TEST_URL is mandatory for Redis cache integration tests"
  runCacheContract(
    "Redis cache contract",
    proc(): Future[KvStore] {.async.} =
      return await openKvStore(getEnv("REDIS_TEST_URL"))
  )

suite "cache failure policy":
  test "propagate returns backend errors and fail-open loads":
    proc exercise() {.async.} =
      let loader: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          return "fallback"
      let strict = newCache(FailingStore())
      expect BackendConnectionError:
        discard await strict.getOrLoad("key", loader)
      await strict.close()

      var options = defaultCacheOptions()
      options.failurePolicy = cfpFailOpen
      let lenient = newCache(FailingStore(), options)
      check (await lenient.getOrLoad("key", loader)) == "fallback"
      check not (await lenient.invalidate("key"))
      check (await lenient.bumpVersion("version")) == 0
      await lenient.close()
    waitFor exercise()

  test "fill write timeouts follow strict and fail-open policy":
    proc exercise() {.async.} =
      let loader: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          return "loaded"
      let strict = newCache(WriteFailStore())
      expect BackendTimeoutError:
        discard await strict.getOrLoad("key", loader)
      await strict.close()
      var options = defaultCacheOptions()
      options.failurePolicy = cfpFailOpen
      let lenient = newCache(WriteFailStore(), options)
      check (await lenient.getOrLoad("key", loader)) == "loaded"
      await lenient.close()
    waitFor exercise()

  test "strict invalidation and version failures retain their typed errors":
    proc exercise() {.async.} =
      let strict = newCache(FailingStore())
      expect BackendConnectionError:
        discard await strict.invalidate("key")
      expect BackendTimeoutError:
        discard await strict.bumpVersion("version")
      await strict.close()
    waitFor exercise()

  test "fail-open never swallows backend cancellation":
    proc exercise() {.async.} =
      var options = defaultCacheOptions()
      options.failurePolicy = cfpFailOpen
      let cache = newCache(CancelledStore(), options)
      let loader: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          return "fallback"
      expect CancelledError:
        discard await cache.getOrLoad("read", loader)
      expect CancelledError:
        discard await cache.invalidate("delete")
      expect CancelledError:
        discard await cache.bumpVersion("increment")
      await cache.close()

      let writeCache = newCache(CancelledWriteStore(), options)
      expect CancelledError:
        discard await writeCache.getOrLoad("write", loader)
      await writeCache.close()
    waitFor exercise()

suite "cache fencing and ownership":
  test "invalidation fences a cancellation-resistant active fill":
    proc exercise() {.async.} =
      let store = await openKvStore("mem://")
      let cache = newCache(store)
      let started = newFuture[void]("fenced loader started")
      let release = newFuture[void]("fenced loader release")
      let loader: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          started.complete()
          await release.noCancel()
          return "stale"
      let fill = cache.getOrLoad("fenced", loader)
      await started
      let invalidating = cache.invalidate("fenced")
      await sleepAsync(chronos.milliseconds(5))
      release.complete()
      discard await invalidating
      expect BackendClosedError:
        discard await fill
      check (await store.get("fenced")).isNone
      await cache.close()
      await store.close()
    waitFor exercise()

  test "close owns a blocked backend read and all waiters":
    proc exercise() {.async.} =
      let started = newFuture[void]("blocked cache read")
      let release = newFuture[void]("release blocked cache read")
      let store = BlockingReadStore(started: started, release: release)
      let cache = newCache(store)
      let loader: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          return "must not load"
      let waiting = cache.getOrLoad("blocked", loader)
      await started
      let first = cache.close()
      let second = cache.close()
      first.cancelSoon()
      await sleepAsync(chronos.milliseconds(5))
      check not second.finished
      release.complete()
      await first
      await second
      expect BackendClosedError:
        discard await waiting
    waitFor exercise()

  test "same-key reentrant loader fails deterministically and retry works":
    proc exercise() {.async.} =
      let store = await openKvStore("mem://")
      let cache = newCache(store)
      var recursive: CacheLoader
      recursive =
        proc(key: string): Future[string] {.async.} =
          return await cache.getOrLoad(key, recursive)
      expect InvalidArgumentError:
        discard await cache.getOrLoad("recursive", recursive)
      let normal: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          return "recovered"
      check (await cache.getOrLoad("recursive", normal)) == "recovered"
      await cache.close()
      await store.close()
    waitFor exercise()

  test "same-key reentrant loader after a yield fails deterministically":
    proc exercise() {.async.} =
      let store = await openKvStore("mem://")
      let cache = newCache(store)
      var recursive: CacheLoader
      recursive =
        proc(key: string): Future[string] {.async.} =
          await sleepAsync(chronos.milliseconds(0))
          return await cache.getOrLoad(key, recursive)
      expect InvalidArgumentError:
        discard await cache.getOrLoad("delayed-recursive", recursive)
      await cache.close()
      await store.close()
    waitFor exercise()

  test "a loader can initiate close without deadlocking cleanup":
    proc exercise() {.async.} =
      let store = await openKvStore("mem://")
      let cache = newCache(store)
      let closeReturned = newFuture[void]("loader close returned")
      let loader: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          await sleepAsync(chronos.milliseconds(0))
          await cache.close()
          closeReturned.complete()
          return "fenced"
      let filling = cache.getOrLoad("loader-close", loader)
      await closeReturned.wait(chronos.milliseconds(100))
      expect BackendClosedError:
        discard await filling
      await cache.close().wait(chronos.milliseconds(100))
      check (await store.get("loader-close")).isNone
      await store.close()
    waitFor exercise()

  test "close compensates a cancellation-resistant backend write":
    proc exercise() {.async.} =
      let writeStarted = newFuture[void]("late cache write started")
      let writeRelease = newFuture[void]("late cache write release")
      let store = LateWriteStore(
        values: initTable[string, string](),
        writeStarted: writeStarted,
        writeRelease: writeRelease
      )
      let cache = newCache(store)
      let loader: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          return "committed-after-close"
      let filling = cache.getOrLoad("late-write", loader)
      await writeStarted
      let closing = cache.close()
      await sleepAsync(chronos.milliseconds(0))
      writeRelease.complete()
      await closing.wait(chronos.milliseconds(100))
      expect BackendClosedError:
        discard await filling
      check (await store.get("late-write")).isNone
    waitFor exercise()

  test "cancelled orphan is fenced before an immediate replacement":
    proc exercise() {.async.} =
      let store = await openKvStore("mem://")
      var options = defaultCacheOptions()
      options.cancellationPolicy = ccpCancelOrphaned
      let cache = newCache(store, options)
      let started = newFuture[void]("old orphan started")
      let release = newFuture[void]("old orphan release")
      let oldLoader: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          started.complete()
          await release.noCancel()
          return "old"
      let oldWaiter = cache.getOrLoad("replacement", oldLoader)
      await started
      oldWaiter.cancelSoon()
      expect CancelledError:
        discard await oldWaiter
      let newLoader: CacheLoader =
        proc(key: string): Future[string] {.async.} =
          return "new"
      check (await cache.getOrLoad("replacement", newLoader)) == "new"
      release.complete()
      await sleepAsync(chronos.milliseconds(5))
      check (await store.get("replacement")) == some("new")
      await cache.close()
      await store.close()
    waitFor exercise()
