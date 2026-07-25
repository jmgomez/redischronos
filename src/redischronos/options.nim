import chronos

type
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
    onHandlerError*: HandlerErrorObserver

func defaultBackendOptions*(): BackendOptions =
  BackendOptions(
    connectTimeout: 5.seconds,
    operationTimeout: 2.seconds,
    memoryMaxEntries: 4096
  )
