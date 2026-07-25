import std/[options, sequtils, tables]
import chronos

import ./api

type
  CacheLoader* = proc(key: string): Future[string] {.gcsafe.}

  CacheCancellationPolicy* = enum
    ccpContinueOrphaned, ccpCancelOrphaned

  CacheFailurePolicy* = enum
    cfpPropagate, cfpFailOpen

  CacheOptions* = object
    ttlSeconds*: int
    cancellationPolicy*: CacheCancellationPolicy
    failurePolicy*: CacheFailurePolicy

  InFlight = ref object
    task: Future[string]
    waiters: int
    cacheGeneration: uint64
    keyGeneration: uint64
    invokingLoader: bool

  Cache* = ref object
    store: KvStore
    options: CacheOptions
    fills: Table[string, InFlight]
    closed: bool
    closeTask: Future[void]
    generation: uint64
    keyGenerations: Table[string, uint64]
    owned: seq[FutureBase]
    activeOperations: int

func defaultCacheOptions*(): CacheOptions =
  CacheOptions(
    ttlSeconds: 0,
    cancellationPolicy: ccpContinueOrphaned,
    failurePolicy: cfpPropagate
  )

proc newCache*(store: KvStore,
    options = defaultCacheOptions()): Cache =
  if store == nil:
    raise newException(InvalidArgumentError, "cache store must not be nil")
  if options.ttlSeconds < 0:
    raise newException(InvalidArgumentError, "cache TTL must not be negative")
  Cache(
    store: store,
    options: options,
    fills: initTable[string, InFlight](),
    keyGenerations: initTable[string, uint64]()
  )

proc requireOpen(cache: Cache) =
  if cache.closed:
    raise newException(BackendClosedError, "cache is closed")

proc versionedKey*(key: string, version: int64): string =
  if key.len == 0:
    raise newException(InvalidArgumentError, "cache key must not be empty")
  if version < 0:
    raise newException(InvalidArgumentError, "cache version must not be negative")
  key & ":v" & $version

proc bumpVersionImpl(cache: Cache, key: string): Future[int64] {.async.} =
  cache.requireOpen()
  if key.len == 0:
    raise newException(InvalidArgumentError, "cache key must not be empty")
  inc cache.keyGenerations.mgetOrPut(key, 0'u64)
  if cache.fills.hasKey(key):
    let entry = cache.fills[key]
    cache.fills.del(key)
    if not entry.task.finished:
      entry.task.cancelSoon()
      await allFutures(entry.task)
  cache.requireOpen()
  let incrementing = cache.store.increment(key)
  cache.owned.add(incrementing)
  try:
    return await incrementing
  except CancelledError:
    raise
  except CatchableError:
    if cache.options.failurePolicy == cfpFailOpen:
      return 0
    raise
  finally:
    cache.owned.keepItIf(it != incrementing)

proc read(cache: Cache, key: string): Future[Option[string]] {.async.} =
  let reading = cache.store.get(key)
  cache.owned.add(reading)
  try:
    return await reading
  except CancelledError:
    raise
  except CatchableError:
    if cache.options.failurePolicy == cfpFailOpen:
      return none(string)
    raise
  finally:
    cache.owned.keepItIf(it != reading)

proc fill(cache: Cache, key: string, loader: CacheLoader,
    entry: InFlight): Future[string] {.async.} =
  try:
    # Yield once so the entry's task is published before loader code can
    # re-enter getOrLoad for the same key.
    await sleepAsync(0.milliseconds)
    entry.invokingLoader = true
    let loading = loader(key)
    entry.invokingLoader = false
    cache.owned.add(loading)
    let value =
      try:
        await loading
      finally:
        cache.owned.keepItIf(it != loading)
    if cache.closed or cache.generation != entry.cacheGeneration or
        cache.keyGenerations.getOrDefault(key) != entry.keyGeneration:
      raise newException(BackendClosedError, "cache fill was fenced")
    let writing =
      cache.store.set(key, value, cache.options.ttlSeconds)
    cache.owned.add(writing)
    try:
      await writing
    except CancelledError:
      raise
    except CatchableError:
      if cache.options.failurePolicy == cfpPropagate:
        raise
    finally:
      cache.owned.keepItIf(it != writing)
    if cache.closed or cache.generation != entry.cacheGeneration or
        cache.keyGenerations.getOrDefault(key) != entry.keyGeneration:
      raise newException(BackendClosedError, "cache fill was fenced")
    return value
  finally:
    if cache.fills.hasKey(key) and cache.fills[key] == entry:
      cache.fills.del(key)

proc getOrLoadImpl(cache: Cache, key: string,
    loader: CacheLoader): Future[string] {.async.} =
  cache.requireOpen()
  if key.len == 0:
    raise newException(InvalidArgumentError, "cache key must not be empty")
  if loader == nil:
    raise newException(InvalidArgumentError, "cache loader must not be nil")

  if cache.fills.hasKey(key):
    let existing = cache.fills[key]
    if existing.invokingLoader:
      raise newException(
        InvalidArgumentError,
        "cache loader must not recursively load its own key"
      )
    if existing.task != nil and not existing.task.finished:
      inc existing.waiters
      try:
        await allFutures(existing.task)
        if cache.closed:
          raise newException(BackendClosedError, "cache is closed")
        return existing.task.read()
      finally:
        dec existing.waiters
        if existing.waiters == 0 and not existing.task.finished and
            cache.options.cancellationPolicy == ccpCancelOrphaned:
          inc cache.keyGenerations.mgetOrPut(key, 0'u64)
          if cache.fills.getOrDefault(key) == existing:
            cache.fills.del(key)
          existing.task.cancelSoon()

  let cached = await cache.read(key)
  if cached.isSome:
    cache.requireOpen()
    return cached.get()
  cache.requireOpen()

  var entry: InFlight
  if cache.fills.hasKey(key):
    entry = cache.fills[key]
  else:
    let keyGeneration = cache.keyGenerations.getOrDefault(key)
    entry = InFlight(
      cacheGeneration: cache.generation,
      keyGeneration: keyGeneration
    )
    cache.fills[key] = entry
    entry.task = cache.fill(key, loader, entry)
  inc entry.waiters
  try:
    await allFutures(entry.task)
    if cache.closed:
      raise newException(BackendClosedError, "cache is closed")
    return entry.task.read()
  finally:
    dec entry.waiters
    if entry.waiters == 0 and not entry.task.finished and
        cache.options.cancellationPolicy == ccpCancelOrphaned:
      inc cache.keyGenerations.mgetOrPut(key, 0'u64)
      if cache.fills.getOrDefault(key) == entry:
        cache.fills.del(key)
      entry.task.cancelSoon()

proc invalidateImpl(cache: Cache, key: string): Future[bool] {.async.} =
  cache.requireOpen()
  if key.len == 0:
    raise newException(InvalidArgumentError, "cache key must not be empty")
  inc cache.keyGenerations.mgetOrPut(key, 0'u64)
  if cache.fills.hasKey(key):
    let entry = cache.fills[key]
    cache.fills.del(key)
    if not entry.task.finished:
      entry.task.cancelSoon()
      await allFutures(entry.task)
  cache.requireOpen()
  let deleting = cache.store.delete(key)
  cache.owned.add(deleting)
  try:
    return await deleting
  except CancelledError:
    raise
  except CatchableError:
    if cache.options.failurePolicy == cfpFailOpen:
      return false
    raise
  finally:
    cache.owned.keepItIf(it != deleting)

proc closeOwned(cache: Cache): Future[void] {.async.} =
  inc cache.generation
  var tasks: seq[FutureBase]
  for _, entry in cache.fills.pairs:
    if entry.task != nil and not entry.task.finished:
      entry.task.cancelSoon()
      tasks.add(entry.task)
  for operation in cache.owned:
    if not operation.finished:
      operation.cancelSoon()
      tasks.add(operation)
  if tasks.len > 0:
    await allFutures(tasks)
  while cache.activeOperations > 0:
    await sleepAsync(0.milliseconds)
  cache.fills.clear()
  cache.owned.setLen(0)
  cache.keyGenerations.clear()

proc joinClose(cache: Cache): Future[void] {.async.} =
  if cache.closeTask == nil:
    cache.closed = true
    cache.closeTask = cache.closeOwned()
  await cache.closeTask.noCancel()

proc close*(cache: Cache): Future[void] =
  cache.joinClose()

proc getOrLoad*(cache: Cache, key: string,
    loader: CacheLoader): Future[string] {.async.} =
  cache.requireOpen()
  inc cache.activeOperations
  try:
    return await cache.getOrLoadImpl(key, loader)
  finally:
    dec cache.activeOperations

proc invalidate*(cache: Cache, key: string): Future[bool] {.async.} =
  cache.requireOpen()
  inc cache.activeOperations
  try:
    return await cache.invalidateImpl(key)
  finally:
    dec cache.activeOperations

proc bumpVersion*(cache: Cache, key: string): Future[int64] {.async.} =
  cache.requireOpen()
  inc cache.activeOperations
  try:
    return await cache.bumpVersionImpl(key)
  finally:
    dec cache.activeOperations
