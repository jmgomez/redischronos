import chronos

type
  ReconnectJitterSource* =
    proc(maxInclusive: int): int {.gcsafe, raises: [].}

  HandlerError* = object
    channel*: string
    subscriptionId*: uint64
    cause*: ref CatchableError

  HandlerErrorObserver* =
    proc(error: HandlerError) {.gcsafe, raises: [].}

  BackendOptions* = object
    connectTimeout*: Duration
    operationTimeout*: Duration
    memoryMaxEntries*: int
    pubSubMaxPendingMessages*: int
    reconnectJitterSource*: ReconnectJitterSource
    onHandlerError*: HandlerErrorObserver

func defaultBackendOptions*(): BackendOptions =
  BackendOptions(
    connectTimeout: 5.seconds,
    operationTimeout: 2.seconds,
    memoryMaxEntries: 4096,
    pubSubMaxPendingMessages: 1024
  )
