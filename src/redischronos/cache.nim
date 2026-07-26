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
    closeCallers: seq[Future[void]]

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

proc containsFuture(root, target: FutureBase): bool =
  var current = root
  var depth = 0
  while current != nil and depth < 1024:
    if current == target:
      return true
    current = current.internalChild
    inc depth

proc activeCloseCaller(cache: Cache, root: FutureBase): Future[void] =
  for caller in cache.closeCallers:
    if not caller.finished and containsFuture(root, caller):
      return caller

proc removeCloseCaller(cache: Cache, caller: Future[void]) =
  cache.closeCallers.keepItIf(it != caller)

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
      let deleting = cache.store.delete(key)
      cache.owned.add(deleting)
      try:
        discard await deleting
      finally:
        cache.owned.keepItIf(it != deleting)
      raise newException(BackendClosedError, "cache fill was fenced")
    return value
  except CancelledError:
    if cache.closed or cache.generation != entry.cacheGeneration or
        cache.keyGenerations.getOrDefault(key) != entry.keyGeneration:
      let deleting = cache.store.delete(key)
      cache.owned.add(deleting)
      try:
        discard await deleting
      finally:
        cache.owned.keepItIf(it != deleting)
      raise newException(BackendClosedError, "cache fill was fenced")
    raise
  finally:
    if cache.fills.hasKey(key) and cache.fills[key] == entry:
      cache.fills.del(key)

proc getOrLoadImpl(cache: Cache, key: string,
    loader: CacheLoader): Future[string] {.async.} =
  # Let the caller publish this future into its await chain before checking
  # whether the call originated from the loader that owns the same fill.
  await sleepAsync(0.milliseconds)
  cache.requireOpen()
  if key.len == 0:
    raise newException(InvalidArgumentError, "cache key must not be empty")
  if loader == nil:
    raise newException(InvalidArgumentError, "cache loader must not be nil")

  if cache.fills.hasKey(key):
    let existing = cache.fills[key]
    if existing.invokingLoader or
        containsFuture(existing.task, chronosInternalRetFuture):
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

  let cached =
    try:
      await cache.read(key)
    except CancelledError:
      if cache.closed:
        raise newException(BackendClosedError, "cache is closed")
      raise
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
  # Let callback/loader callers attach the returned close future before
  # inspecting owned-task ancestry.
  await sleepAsync(0.milliseconds)
  inc cache.generation
  var tasks: seq[FutureBase]
  for _, entry in cache.fills.pairs:
    if entry.task != nil and not entry.task.finished:
      let caller = cache.activeCloseCaller(entry.task)
      if caller != nil:
        caller.complete()
      else:
        entry.task.cancelSoon()
      tasks.add(entry.task)
  for operation in cache.owned:
    if not operation.finished:
      let caller = cache.activeCloseCaller(operation)
      if caller != nil:
        caller.complete()
      else:
        operation.cancelSoon()
      tasks.add(operation)
  if tasks.len > 0:
    await allFutures(tasks)
  while cache.activeOperations > 0:
    await sleepAsync(0.milliseconds)
  cache.fills.clear()
  cache.owned.setLen(0)
  cache.keyGenerations.clear()

proc ensureClose(cache: Cache) =
  if cache.closeTask == nil:
    cache.closed = true
    cache.closeTask = cache.closeOwned()

proc close*(cache: Cache): Future[void] =
  cache.ensureClose()
  let caller = newFuture[void](
    "cache close caller",
    {FutureFlag.OwnCancelSchedule}
  )
  caller.cancelCallback = nil
  cache.closeCallers.add(caller)
  proc finishCaller(_: pointer) {.gcsafe, raises: [].} =
    if not caller.finished:
      if cache.closeTask.failed:
        caller.fail(cache.closeTask.error)
      elif cache.closeTask.cancelled:
        caller.cancelSoon()
      else:
        caller.complete()
  proc pruneCaller(_: pointer) {.gcsafe, raises: [].} =
    cache.removeCloseCaller(caller)
  cache.closeTask.addCallback(finishCaller, nil)
  caller.addCallback(pruneCaller, nil)
  result = caller

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
