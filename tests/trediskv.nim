import std/[options, os, unittest]
import chronos
import redischronos

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
