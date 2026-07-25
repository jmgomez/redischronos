import std/[options, strutils, tables]
import chronos

import ./api
import ./errors

type
  MemoryClock* = proc(): Moment {.gcsafe.}

  Entry = object
    value: string
    expiresAt: Option[Moment]
    lastAccess: uint64

  InMemoryKvStore* = ref object of KvStore
    entries: Table[string, Entry]
    isClosed: bool
    clock: MemoryClock
    maxEntries: int
    accessCounter: uint64

proc systemClock(): Moment {.gcsafe.} =
  Moment.now()

proc newInMemoryKvStore*(clock: MemoryClock = systemClock,
    maxEntries = 4096): KvStore =
  if maxEntries <= 0:
    raise newException(
      InvalidArgumentError,
      "memoryMaxEntries must be positive"
    )
  InMemoryKvStore(
    entries: initTable[string, Entry](),
    clock: clock,
    maxEntries: maxEntries
  )

proc requireOpen(store: InMemoryKvStore) =
  if store.isClosed:
    raise newException(BackendClosedError, "key/value backend is closed")

proc requireKey(key: string) =
  if key.len == 0:
    raise newException(InvalidArgumentError, "key must not be empty")

proc removeIfExpired(store: InMemoryKvStore, key: string) =
  if store.entries.hasKey(key):
    let expiresAt = store.entries[key].expiresAt
    if expiresAt.isSome and expiresAt.get <= store.clock():
      store.entries.del(key)

proc nextAccess(store: InMemoryKvStore): uint64 =
  inc store.accessCounter
  store.accessCounter

proc purgeExpired(store: InMemoryKvStore) =
  var expiredKeys: seq[string]
  let now = store.clock()
  for key, entry in store.entries.pairs:
    if entry.expiresAt.isSome and entry.expiresAt.get <= now:
      expiredKeys.add(key)
  for key in expiredKeys:
    store.entries.del(key)

proc evictOne(store: InMemoryKvStore) =
  var victim = ""
  var victimAccess = high(uint64)
  for key, entry in store.entries.pairs:
    if entry.lastAccess < victimAccess or
        (entry.lastAccess == victimAccess and (victim.len == 0 or key < victim)):
      victim = key
      victimAccess = entry.lastAccess
  if victim.len > 0:
    store.entries.del(victim)

method get*(store: InMemoryKvStore,
    key: string): Future[Option[string]] {.async.} =
  store.requireOpen()
  requireKey(key)
  store.removeIfExpired(key)
  if store.entries.hasKey(key):
    store.entries[key].lastAccess = store.nextAccess()
    return some(store.entries[key].value)
  return none(string)

method set*(store: InMemoryKvStore, key, value: string,
    ttlSeconds = 0): Future[void] {.async.} =
  store.requireOpen()
  requireKey(key)
  if ttlSeconds < 0:
    raise newException(InvalidArgumentError, "TTL must not be negative")
  let expiresAt =
    if ttlSeconds == 0: none(Moment)
    else: some(store.clock() + ttlSeconds.seconds)
  if not store.entries.hasKey(key):
    store.purgeExpired()
    if store.entries.len >= store.maxEntries:
      store.evictOne()
  store.entries[key] = Entry(
    value: value,
    expiresAt: expiresAt,
    lastAccess: store.nextAccess()
  )

method delete*(store: InMemoryKvStore, key: string): Future[bool] {.async.} =
  store.requireOpen()
  requireKey(key)
  store.removeIfExpired(key)
  result = store.entries.hasKey(key)
  if result:
    store.entries.del(key)

method exists*(store: InMemoryKvStore, key: string): Future[bool] {.async.} =
  store.requireOpen()
  requireKey(key)
  store.removeIfExpired(key)
  return store.entries.hasKey(key)

method increment*(store: InMemoryKvStore, key: string): Future[int64] {.async.} =
  store.requireOpen()
  requireKey(key)
  store.removeIfExpired(key)
  if not store.entries.hasKey(key):
    store.purgeExpired()
    if store.entries.len >= store.maxEntries:
      store.evictOne()
    store.entries[key] = Entry(value: "1", lastAccess: store.nextAccess())
    return 1

  let currentValue = store.entries[key].value
  var current: int64
  try:
    current = parseBiggestInt(currentValue)
  except ValueError:
    raise newException(
      RedisCommandError,
      "value is not an integer or out of range"
    )
  if current == high(int64):
    raise newException(
      RedisCommandError,
      "increment would overflow a signed 64-bit integer"
    )

  result = current + 1
  let expiresAt = store.entries[key].expiresAt
  store.entries[key] = Entry(
    value: $result,
    expiresAt: expiresAt,
    lastAccess: store.nextAccess()
  )

method close*(store: InMemoryKvStore): Future[void] {.async.} =
  if not store.isClosed:
    store.isClosed = true
    store.entries.clear()

when defined(test):
  proc setLastAccessForTest*(store: KvStore, key: string, access: uint64) =
    let memoryStore = InMemoryKvStore(store)
    memoryStore.entries[key].lastAccess = access
