import std/unittest
import redischronos

suite "redischronos package":
  test "exposes its development version":
    check redischronosVersion == "0.1.0-dev"

