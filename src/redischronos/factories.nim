import std/strutils
import chronos

import ./api
import ./errors
import ./memorykv
import ./memorypubsub
import ./options
import ./rediskv
import ./redispubsub

proc openKvStore*(url = "mem://",
    options = defaultBackendOptions()): Future[KvStore] {.async.} =
  if url.len == 0 or url == "mem://":
    return newInMemoryKvStore(maxEntries = options.memoryMaxEntries)
  if url.startsWith("redis://"):
    return newRedisKvStore(url, options)
  raise newException(
    InvalidBackendUrlError,
    "unsupported backend URL scheme"
  )

proc openPubSub*(url = "mem://",
    options = defaultBackendOptions()): Future[PubSub] {.async.} =
  if url.len == 0 or url == "mem://":
    return newInProcessPubSub(options)
  if url.startsWith("redis://"):
    return await newRedisPubSub(url, options)
  raise newException(
    InvalidBackendUrlError,
    "unsupported backend URL scheme"
  )
