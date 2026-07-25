import std/[options, tables]
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

  Cache* = ref object
    store: KvStore
    options: CacheOptions
    fills: Table[string, InFlight]
    closed: bool
    closeTask: Future[void]

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
    fills: initTable[string, InFlight]()
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

proc bumpVersion*(cache: Cache, key: string): Future[int64] {.async.} =
  cache.requireOpen()
  if key.len == 0:
    raise newException(InvalidArgumentError, "cache key must not be empty")
  return await cache.store.increment(key)

proc read(cache: Cache, key: string): Future[Option[string]] {.async.} =
  try:
    return await cache.store.get(key)
  except CancelledError:
    raise
  except CatchableError:
    if cache.options.failurePolicy == cfpFailOpen:
      return none(string)
    raise

proc fill(cache: Cache, key: string, loader: CacheLoader,
    entry: InFlight): Future[string] {.async.} =
  try:
    let value = await loader(key)
    try:
      await cache.store.set(key, value, cache.options.ttlSeconds)
    except CancelledError:
      raise
    except CatchableError:
      if cache.options.failurePolicy == cfpPropagate:
        raise
    return value
  finally:
    if cache.fills.hasKey(key) and cache.fills[key] == entry:
      cache.fills.del(key)

proc getOrLoad*(cache: Cache, key: string,
    loader: CacheLoader): Future[string] {.async.} =
  cache.requireOpen()
  if key.len == 0:
    raise newException(InvalidArgumentError, "cache key must not be empty")
  if loader == nil:
    raise newException(InvalidArgumentError, "cache loader must not be nil")

  let cached = await cache.read(key)
  if cached.isSome:
    return cached.get()
  cache.requireOpen()

  var entry: InFlight
  if cache.fills.hasKey(key):
    entry = cache.fills[key]
  else:
    entry = InFlight()
    cache.fills[key] = entry
    entry.task = cache.fill(key, loader, entry)
  inc entry.waiters
  try:
    await allFutures(entry.task)
    return entry.task.read()
  finally:
    dec entry.waiters
    if entry.waiters == 0 and not entry.task.finished and
        cache.options.cancellationPolicy == ccpCancelOrphaned:
      entry.task.cancelSoon()

proc invalidate*(cache: Cache, key: string): Future[bool] {.async.} =
  cache.requireOpen()
  if key.len == 0:
    raise newException(InvalidArgumentError, "cache key must not be empty")
  try:
    return await cache.store.delete(key)
  except CancelledError:
    raise
  except CatchableError:
    if cache.options.failurePolicy == cfpFailOpen:
      return false
    raise

proc closeOwned(cache: Cache): Future[void] {.async.} =
  var tasks: seq[Future[string]]
  for _, entry in cache.fills.pairs:
    if entry.task != nil and not entry.task.finished:
      entry.task.cancelSoon()
      tasks.add(entry.task)
  for task in tasks:
    try:
      discard await task
    except CancelledError:
      discard
    except CatchableError:
      discard
  cache.fills.clear()

proc joinClose(cache: Cache): Future[void] {.async.} =
  if cache.closeTask == nil:
    cache.closed = true
    cache.closeTask = cache.closeOwned()
  await cache.closeTask.noCancel()

proc close*(cache: Cache): Future[void] =
  cache.joinClose()
