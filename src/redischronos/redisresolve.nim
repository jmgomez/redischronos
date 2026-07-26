import std/[atomics, monotimes, sets, tables]
when defined(test):
  import std/os
import chronos

import ./errors

const ResolverQueueLimit = 32

type
  ResolverRequest = object
    id: uint64
    host: string
    port: uint16
    deadlineTicks: int64

  ResolverResponse = object
    id: uint64
    addresses: seq[TransportAddress]

  ResolverWorkerState = object
    requests: Channel[ResolverRequest]
    responses: Channel[ResolverResponse]
    delayMilliseconds: Atomic[int]
    workersCreated: Atomic[int]
    workersAlive: Atomic[int]

var
  resolverState {.threadvar.}: ptr ResolverWorkerState
  resolverThread {.threadvar.}: Thread[ptr ResolverWorkerState]
  nextRequestId {.threadvar.}: uint64
  activeRequests {.threadvar.}: HashSet[uint64]
  completedResponses {.threadvar.}: Table[
    uint64, seq[TransportAddress]]

proc resolveWorker(host: string,
    port: uint16): seq[TransportAddress] {.gcsafe.} =
  try:
    result = resolveTAddress(host, Port(port))
  except CatchableError:
    result = @[]

proc resolverLoop(state: ptr ResolverWorkerState) {.thread.} =
  discard state.workersCreated.fetchAdd(1)
  discard state.workersAlive.fetchAdd(1)
  try:
    while true:
      let request = state.requests.recv()
      if getMonoTime().ticks >= request.deadlineTicks:
        continue
      when defined(test):
        let delay = state.delayMilliseconds.load()
        if delay > 0:
          sleep(delay)
      let addresses = resolveWorker(request.host, request.port)
      if getMonoTime().ticks < request.deadlineTicks:
        discard state.responses.trySend(ResolverResponse(
          id: request.id,
          addresses: addresses
        ))
  finally:
    discard state.workersAlive.fetchSub(1)

proc ensureResolverStarted(): ptr ResolverWorkerState =
  if resolverState == nil:
    resolverState = cast[ptr ResolverWorkerState](
      allocShared0(sizeof(ResolverWorkerState))
    )
    resolverState.requests.open(ResolverQueueLimit)
    resolverState.responses.open(ResolverQueueLimit)
    activeRequests = initHashSet[uint64]()
    completedResponses =
      initTable[uint64, seq[TransportAddress]]()
    createThread(resolverThread, resolverLoop, resolverState)
  resolverState

proc drainResponses(state: ptr ResolverWorkerState) =
  while true:
    let received = state.responses.tryRecv()
    if not received.dataAvailable:
      break
    let response = received.msg
    if response.id in activeRequests:
      completedResponses[response.id] = response.addresses

proc resolveRedisAddresses*(host: string, port: uint16,
    timeout: Duration): Future[seq[TransportAddress]] {.async.} =
  if timeout <= 0.nanoseconds:
    raise newException(BackendTimeoutError, "Redis host resolution timed out")
  let state = ensureResolverStarted()
  inc nextRequestId
  let requestId = nextRequestId
  let deadline = Moment.now() + timeout
  let request = ResolverRequest(
    id: requestId,
    host: host,
    port: port,
    deadlineTicks: getMonoTime().ticks + timeout.nanoseconds
  )
  activeRequests.incl(requestId)
  if not state.requests.trySend(request):
    activeRequests.excl(requestId)
    raise newException(
      BackendConnectionError,
      "Redis host resolver is at capacity"
    )
  try:
    while true:
      state.drainResponses()
      if completedResponses.hasKey(requestId):
        result = completedResponses[requestId]
        completedResponses.del(requestId)
        if result.len == 0:
          raise newException(
            BackendConnectionError,
            "Redis host resolution failed"
          )
        return
      if Moment.now() >= deadline:
        raise newException(
          BackendTimeoutError,
          "Redis host resolution timed out"
        )
      await sleepAsync(1.milliseconds)
  finally:
    activeRequests.excl(requestId)
    completedResponses.del(requestId)

when defined(test):
  proc setResolverDelayForTest*(milliseconds: int) =
    ensureResolverStarted().delayMilliseconds.store(max(milliseconds, 0))

  proc resolverStateForTest*(): (int, int, int) =
    let state = ensureResolverStarted()
    (
      state.workersAlive.load(),
      max(state.requests.peek(), 0),
      max(state.responses.peek(), 0)
    )

  proc resolverWorkersCreatedForTest*(): int =
    ensureResolverStarted().workersCreated.load()
