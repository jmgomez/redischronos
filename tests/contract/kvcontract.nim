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
        var increments: seq[Future[int64]]
        for _ in 0 ..< 200:
          increments.add(store.increment("counter"))
        for increment in increments:
          discard await increment
        check (await store.get("counter")) == some("200")
        for value in ["invalid", $high(int64)]:
          await store.set("counter", value)
          expect RedisCommandError:
            discard await store.increment("counter")
          check (await store.get("counter")) == some(value)
        await store.close()
      waitFor exercise()

    test "close is idempotent and later operations fail":
      let store = waitFor factory()
      waitFor store.close()
      waitFor store.close()
      expect BackendClosedError:
        discard waitFor store.get("key")
