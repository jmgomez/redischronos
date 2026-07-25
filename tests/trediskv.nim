import std/[options, os, unittest]
import chronos
import redischronos
import redischronos/redisconnection
import redischronos/redisurl
import redischronos/resp2

import ./contract/kvcontract

when defined(redisIntegration):
  doAssert getEnv("REDIS_TEST_URL").len > 0,
    "REDIS_TEST_URL is mandatory for Redis integration tests"

  proc openRedisKv(): Future[KvStore] {.gcsafe.} =
    openKvStore(getEnv("REDIS_TEST_URL"))

  kvContractSuite("Redis", openRedisKv)

  suite "Redis KV mappings":
    test "factory selects Redis using only the URL":
      let store = waitFor openKvStore(getEnv("REDIS_TEST_URL"))
      waitFor store.set("redischronos:mapping", "value", 5)
      check (waitFor store.get("redischronos:mapping")).get == "value"
      discard waitFor store.delete("redischronos:mapping")
      waitFor store.close()

    test "concurrent close callers share terminal cleanup":
      let store = waitFor openKvStore(getEnv("REDIS_TEST_URL"))
      let first = store.close()
      let second = store.close()
      first.cancelSoon()
      waitFor first
      waitFor second
      waitFor store.close()

    test "SELECT isolates the configured Redis database":
      proc exercise() {.async.} =
        var selected = parseRedisUrl(getEnv("REDIS_TEST_URL"))
        let selectedConnection = newRedisConnection(selected)
        selected.database =
          if selected.database == 15: 14 else: selected.database + 1
        let otherConnection = newRedisConnection(selected)
        discard await selectedConnection.execute([
          "SET", "redischronos:select-isolation", "selected"
        ])
        check (await otherConnection.execute([
          "GET", "redischronos:select-isolation"
        ])).kind == rkNilBulkString
        discard await selectedConnection.execute([
          "DEL", "redischronos:select-isolation"
        ])
        await selectedConnection.close()
        await otherConnection.close()
      waitFor exercise()
