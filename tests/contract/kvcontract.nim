import std/[options, unittest]
import chronos
import redischronos

type KvFactory* = proc(): Future[KvStore] {.gcsafe.}

proc probeKvContract*(factory: KvFactory): Future[seq[string]] {.async.} =
  let store = await factory()
  if (await store.get("missing")).isSome:
    result.add("missing get")
  await store.set("roundtrip", "before\0after\xFF")
  if (await store.get("roundtrip")) != some("before\0after\xFF"):
    result.add("round trip")
  if not (await store.delete("roundtrip")):
    result.add("delete")
  if (await store.increment("counter")) != 1:
    result.add("increment")
  await store.close()

template kvContractSuite*(backendName: string, factory: KvFactory) =
  suite backendName & " KV contract":
    test "missing, round-trip, overwrite, exists, and delete":
      let store = waitFor factory()
      discard waitFor store.delete("missing")
      discard waitFor store.delete("key")
      check (waitFor store.get("missing")).isNone
      let value = "before\0after\xFF"
      waitFor store.set("key", value)
      check (waitFor store.get("key")) == some(value)
      waitFor store.set("key", "")
      check (waitFor store.get("key")) == some("")
      check waitFor store.exists("key")
      check waitFor store.delete("key")
      check not (waitFor store.delete("key"))
      check not (waitFor store.exists("key"))
      waitFor store.close()

    test "invalid keys and TTL are typed failures":
      let store = waitFor factory()
      expect InvalidArgumentError:
        discard waitFor store.get("")
      waitFor store.set("key", "original")
      expect InvalidArgumentError:
        waitFor store.set("key", "replacement", -1)
      check (waitFor store.get("key")) == some("original")
      waitFor store.close()

    test "increment is atomic and preserves invalid values":
      proc exercise() {.async.} =
        let store = await factory()
        discard await store.delete("counter")
        var increments: seq[Future[int64]]
        for _ in 0 ..< 50:
          increments.add(store.increment("counter"))
        for increment in increments:
          discard await increment
        check (await store.get("counter")) == some("50")
        for value in ["invalid", $high(int64)]:
          await store.set("counter", value)
          expect RedisCommandError:
            discard await store.increment("counter")
          check (await store.get("counter")) == some(value)
        await store.close()
      waitFor exercise()

    test "positive TTL expires":
      proc exercise() {.async.} =
        let store = await factory()
        await store.set("expiring-contract-key", "value", 1)
        check (await store.get("expiring-contract-key")) == some("value")
        await sleepAsync(1100.milliseconds)
        check (await store.get("expiring-contract-key")).isNone
        await store.close()
      waitFor exercise()

    test "increment preserves an existing TTL":
      proc exercise() {.async.} =
        let store = await factory()
        await store.set("expiring-contract-counter", "1", 1)
        check (await store.increment("expiring-contract-counter")) == 2
        await sleepAsync(1100.milliseconds)
        check (await store.get("expiring-contract-counter")).isNone

        for value in ["invalid", $high(int64)]:
          await store.set("expiring-contract-counter", value, 1)
          expect RedisCommandError:
            discard await store.increment("expiring-contract-counter")
          check (await store.get("expiring-contract-counter")) == some(value)
        await store.close()
      waitFor exercise()

    test "close is idempotent and later operations fail":
      let store = waitFor factory()
      waitFor store.close()
      waitFor store.close()
      expect BackendClosedError:
        discard waitFor store.get("key")
