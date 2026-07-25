import std/threadpool
import chronos

import ./errors

proc resolveWorker(host: string, port: uint16): bool {.gcsafe.} =
  try:
    result = resolveTAddress(host, Port(port)).len > 0
  except CatchableError:
    result = false

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
  if not ^pending:
    raise newException(BackendConnectionError, "Redis host resolution failed")
  try:
    result = resolveTAddress(host, Port(port))
  except CatchableError:
    raise newException(BackendConnectionError, "Redis host resolution failed")
  if result.len == 0:
    raise newException(BackendConnectionError, "Redis host resolution failed")
