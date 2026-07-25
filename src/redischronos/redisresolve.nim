import std/threadpool
import chronos

import ./errors

proc resolveWorker(host: string, port: uint16): seq[TransportAddress] {.gcsafe.} =
  try:
    result = resolveTAddress(host, Port(port))
  except CatchableError:
    result = @[]

proc resolveRedisAddresses*(host: string, port: uint16,
    timeout: Duration): Future[seq[TransportAddress]] {.async.} =
  if timeout <= 0.nanoseconds:
    raise newException(BackendTimeoutError, "Redis host resolution timed out")
  let pending = spawn resolveWorker(host, port)
  let deadline = Moment.now() + timeout
  while not pending.isReady:
    if Moment.now() >= deadline:
      raise newException(BackendTimeoutError, "Redis host resolution timed out")
    await sleepAsync(1.milliseconds)
  result = ^pending
  if result.len == 0:
    raise newException(BackendConnectionError, "Redis host resolution failed")
