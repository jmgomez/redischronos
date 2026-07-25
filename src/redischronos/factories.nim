import chronos

import ./api
import ./errors
import ./memorykv
import ./memorypubsub
import ./options

proc normalizeMemoryUrl(url: string) =
  if url.len == 0 or url == "mem://":
    return
  raise newException(
    InvalidBackendUrlError,
    "unsupported backend URL scheme"
  )

proc openKvStore*(url = "mem://",
    options = defaultBackendOptions()): Future[KvStore] {.async.} =
  normalizeMemoryUrl(url)
  return newInMemoryKvStore(maxEntries = options.memoryMaxEntries)

proc openPubSub*(url = "mem://",
    options = defaultBackendOptions()): Future[PubSub] {.async.} =
  normalizeMemoryUrl(url)
  return newInProcessPubSub(options)
