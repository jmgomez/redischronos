import std/[options, unittest]
import chronos
import redischronos
import redischronos/memorykv

import ./contract/kvcontract

proc openMemoryKv(): Future[KvStore] {.gcsafe.} =
  openKvStore("mem://")

kvContractSuite("memory", openMemoryKv)

suite "memory KV internals":
  test "TTL expiry is deterministic and overwrite resets it":
    var now = Moment.now()
    let clock: MemoryClock = proc(): Moment = now
    let store = newInMemoryKvStore(clock)
    waitFor store.set("persistent", "value")
    waitFor store.set("expiring", "old", 2)
    now += 1.seconds
    waitFor store.set("expiring", "new", 3)
    now += 2.seconds
    check (waitFor store.get("persistent")) == some("value")
    check (waitFor store.get("expiring")) == some("new")
    now += 1.seconds
    check (waitFor store.get("expiring")).isNone
    check not (waitFor store.exists("expiring"))
    check (waitFor store.increment("expiring")) == 1
    waitFor store.close()

  test "LRU reads and overwrites refresh recency":
    let store = newInMemoryKvStore(maxEntries = 2)
    waitFor store.set("a", "1")
    waitFor store.set("b", "2")
    discard waitFor store.get("a")
    waitFor store.set("c", "3")
    check (waitFor store.get("b")).isNone
    waitFor store.set("a", "updated")
    waitFor store.set("d", "4")
    check (waitFor store.get("c")).isNone
    check (waitFor store.get("a")) == some("updated")
    waitFor store.close()

  test "expired entries are purged before live eviction":
    var now = Moment.now()
    let clock: MemoryClock = proc(): Moment = now
    let store = newInMemoryKvStore(clock, maxEntries = 2)
    waitFor store.set("live", "1")
    waitFor store.set("expired", "2", 1)
    now += 1.seconds
    waitFor store.set("new", "3")
    check (waitFor store.get("live")).isSome
    check (waitFor store.get("expired")).isNone
    waitFor store.close()

  test "LRU ties use lexical keys":
    let store = newInMemoryKvStore(maxEntries = 2)
    waitFor store.set("z", "last")
    waitFor store.set("a", "first")
    store.setLastAccessForTest("z", 1)
    store.setLastAccessForTest("a", 1)
    waitFor store.set("new", "value")
    check (waitFor store.get("a")).isNone
    check (waitFor store.get("z")).isSome
    waitFor store.close()

  test "non-positive capacity is rejected":
    var options = defaultBackendOptions()
    for capacity in [0, -1]:
      options.memoryMaxEntries = capacity
      expect InvalidArgumentError:
        discard waitFor openKvStore("mem://", options)
