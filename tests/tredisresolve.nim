import std/unittest
import chronos

import redischronos
import redischronos/redisresolve

suite "bounded Redis resolver":
  test "many timed-out resolutions keep workers and queues bounded":
    proc exercise() {.async.} =
      setResolverDelayForTest(50)
      var resolutions: seq[Future[seq[TransportAddress]]]
      for _ in 0 ..< 100:
        resolutions.add(
          resolveRedisAddresses("localhost", 6379, 2.milliseconds)
        )
      var eventLoopTicks = 0
      let ticker = proc() {.async.} =
        for _ in 0 ..< 10:
          await sleepAsync(1.milliseconds)
          inc eventLoopTicks
      let ticking = ticker()
      for resolution in resolutions:
        try:
          discard await resolution
          check false
        except BackendTimeoutError, BackendConnectionError:
          discard
      await ticking
      check eventLoopTicks == 10
      let busy = resolverStateForTest()
      check busy[0] == 1
      check busy[1] <= 32
      check busy[2] <= 32
      setResolverDelayForTest(0)
      let drainDeadline = Moment.now() + 2.seconds
      var drained = resolverStateForTest()
      while drained[1] > 0 and Moment.now() < drainDeadline:
        await sleepAsync(10.milliseconds)
        drained = resolverStateForTest()
      check drained[0] == 1
      check drained[1] == 0
      check drained[2] <= 32
    waitFor exercise()
