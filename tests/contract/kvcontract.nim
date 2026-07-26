import std/[options, os, unittest]
import chronos
import redischronos

type KvFactory* = proc(): Future[KvStore] {.gcsafe.}

var kvProbeCounter: uint64

proc probeKvContract*(factory: KvFactory): Future[seq[string]] {.async.} =
  let store = await factory()
  inc kvProbeCounter
  let prefix = "redischronos:probe:" & $getCurrentProcessId() & ":" &
    $kvProbeCounter & ":"
  let missing = prefix & "missing"
  let roundTrip = prefix & "roundtrip"
  let counter = prefix & "counter"
  let expiring = prefix & "expiring"
  let expiringCounter = prefix & "expiring-counter"
  if (await store.get(missing)).isSome:
    result.add("missing get")
  await store.set(roundTrip, "before\0after\xFF")
  if (await store.get(roundTrip)) != some("before\0after\xFF"):
    result.add("round trip")
  try:
    if not (await store.exists(roundTrip)):
      result.add("exists")
  except CatchableError:
    result.add("exists")
  if not (await store.delete(roundTrip)):
    result.add("delete existing")
  if await store.delete(prefix & "missing-delete"):
    result.add("delete missing")
  if (await store.increment(counter)) != 1:
    result.add("increment missing")
  await store.set(roundTrip, "first")
  await store.set(roundTrip, "overwritten")
  if (await store.get(roundTrip)) != some("overwritten"):
    result.add("overwrite")
  await store.set(roundTrip, "")
  if (await store.get(roundTrip)) != some(""):
    result.add("empty value")
  try:
    discard await store.get("")
    result.add("invalid key")
  except InvalidArgumentError:
    discard
  except CatchableError:
    result.add("invalid key")
  try:
    await store.set(roundTrip, "invalid ttl", -1)
    result.add("negative ttl")
  except InvalidArgumentError:
    discard
  except CatchableError:
    result.add("negative ttl")
  var increments: seq[Future[int64]]
  for _ in 0 ..< 20:
    increments.add(store.increment(counter))
  for increment in increments:
    discard await increment
  if (await store.get(counter)) != some("21"):
    result.add("atomic increment")
  for value in ["invalid", $high(int64)]:
    await store.set(counter, value)
    try:
      discard await store.increment(counter)
      result.add(
        if value == "invalid": "invalid increment" else: "overflow increment"
      )
    except RedisCommandError:
      discard
    except CatchableError:
      result.add(
        if value == "invalid": "invalid increment" else: "overflow increment"
      )
    if (await store.get(counter)) != some(value):
      result.add(
        if value == "invalid":
          "invalid value preservation"
        else:
          "overflow value preservation"
      )
  await store.set(expiring, "value", 1)
  await store.set(expiringCounter, "1", 1)
  try:
    if (await store.increment(expiringCounter)) != 2:
      result.add("TTL increment")
  except CatchableError:
    result.add("TTL increment")
  let expiryDeadline = Moment.now() + chronos.seconds(2)
  while Moment.now() < expiryDeadline:
    if (await store.get(expiring)).isNone and
        (await store.get(expiringCounter)).isNone:
      break
    await sleepAsync(chronos.milliseconds(10))
  if (await store.get(expiring)).isSome:
    result.add("TTL expiry")
  if (await store.get(expiringCounter)).isSome:
    result.add("increment TTL preservation")
  for key in [roundTrip, counter, expiring, expiringCounter]:
    discard await store.delete(key)
  await store.close()
  try:
    await store.close()
  except CatchableError:
    result.add("idempotent close")
  try:
    discard await store.get(prefix & "closed")
    result.add("closed lifecycle")
  except BackendClosedError:
    discard
  except CatchableError:
    result.add("closed lifecycle")

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
          await sleepAsync(1100.milliseconds)
          check (await store.get("expiring-contract-counter")).isNone
        await store.close()
      waitFor exercise()

    test "close is idempotent and later operations fail":
      let store = waitFor factory()
      waitFor store.close()
      waitFor store.close()
      expect BackendClosedError:
        discard waitFor store.get("key")
