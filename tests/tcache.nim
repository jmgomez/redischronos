import std/[options, os, times, unittest]
import chronos

import redischronos
import redischronos/cache

type
  StoreFactory = proc(): Future[KvStore] {.gcsafe.}
  FailingStore = ref object of KvStore

method get(store: FailingStore,
    key: string): Future[Option[string]] {.async.} =
  raise newException(BackendConnectionError, "injected read failure")

method set(store: FailingStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  raise newException(BackendConnectionError, "injected write failure")

method delete(store: FailingStore, key: string): Future[bool] {.async.} =
  raise newException(BackendConnectionError, "injected delete failure")

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
        expect CancelledError:
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

runCacheContract(
  "memory cache contract",
  proc(): Future[KvStore] {.async.} =
    return await openKvStore("mem://")
)

when defined(redisIntegration):
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
      await lenient.close()
    waitFor exercise()
