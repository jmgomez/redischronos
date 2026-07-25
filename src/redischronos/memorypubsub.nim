import std/[deques, sequtils, tables]
import chronos

import ./api
import ./errors
import ./options

type
  MemorySubscription = ref object of Subscription
    id: uint64
    channel: string
    handler: MessageHandler
    active: bool
    queue: Deque[string]
    worker: Future[void]
    owner: InProcessPubSub

  InProcessPubSub* = ref object of PubSub
    channels: Table[string, seq[MemorySubscription]]
    subscriptions: seq[MemorySubscription]
    retiring: seq[MemorySubscription]
    nextId: uint64
    isClosed: bool
    options: BackendOptions
    currentStateHandler: StateHandler
    stateQueue: Deque[ConnectionState]
    stateWorker: Future[void]
    closeTask: Future[void]

proc newInProcessPubSub*(options: BackendOptions): PubSub =
  if options.pubSubMaxPendingMessages <= 0:
    raise newException(
      InvalidArgumentError,
      "pubSubMaxPendingMessages must be positive"
    )
  InProcessPubSub(
    channels: initTable[string, seq[MemorySubscription]](),
    options: options
  )

proc requireOpen(bus: InProcessPubSub) =
  if bus.isClosed:
    raise newException(BackendClosedError, "pub/sub backend is closed")

proc requireChannel(channel: string) =
  if channel.len == 0:
    raise newException(InvalidArgumentError, "channel must not be empty")

proc deliver(subscription: MemorySubscription) {.async.} =
  while subscription.active and subscription.queue.len > 0:
    let payload = subscription.queue.popFirst()
    if not subscription.active:
      break
    try:
      await subscription.handler(subscription.channel, payload)
    except CancelledError:
      raise
    except CatchableError:
      if subscription.owner.options.onHandlerError != nil:
        subscription.owner.options.onHandlerError(HandlerError(
          channel: subscription.channel,
          subscriptionId: subscription.id,
          cause: newException(ValueError, "message handler failed")
        ))

proc startWorker(subscription: MemorySubscription) =
  if subscription.worker == nil or subscription.worker.finished:
    subscription.worker = deliver(subscription)

proc observeStates(bus: InProcessPubSub) {.async.} =
  while bus.stateQueue.len > 0:
    let state = bus.stateQueue.popFirst()
    if bus.currentStateHandler != nil:
      try:
        await bus.currentStateHandler(state)
      except CancelledError:
        raise
      except CatchableError:
        discard

proc notifyState(bus: InProcessPubSub, state: ConnectionState) =
  bus.stateQueue.addLast(state)
  if bus.stateWorker == nil or bus.stateWorker.finished:
    bus.stateWorker = bus.observeStates()

method subscribe*(bus: InProcessPubSub, channel: string,
    handler: MessageHandler): Future[Subscription] {.async.} =
  bus.requireOpen()
  requireChannel(channel)
  if handler == nil:
    raise newException(InvalidArgumentError, "message handler must not be nil")
  inc bus.nextId
  let subscription = MemorySubscription(
    id: bus.nextId,
    channel: channel,
    handler: handler,
    active: true,
    owner: bus
  )
  bus.channels.mgetOrPut(channel, @[]).add(subscription)
  bus.subscriptions.add(subscription)
  return subscription

method unsubscribe*(bus: InProcessPubSub,
    subscription: Subscription): Future[void] {.async.} =
  bus.requireOpen()
  if subscription == nil:
    return
  if subscription of MemorySubscription:
    let memorySubscription = MemorySubscription(subscription)
    if memorySubscription.owner == bus and memorySubscription.active:
      memorySubscription.active = false
      memorySubscription.queue.clear()
      if memorySubscription.worker != nil and
          not memorySubscription.worker.finished:
        bus.retiring.add(memorySubscription)
      let channel = memorySubscription.channel
      bus.subscriptions.keepItIf(it != memorySubscription)
      if bus.channels.hasKey(channel):
        bus.channels[channel].keepItIf(it != memorySubscription)
        if bus.channels[channel].len == 0:
          bus.channels.del(channel)

method publish*(bus: InProcessPubSub, channel,
    payload: string): Future[int64] {.async.} =
  bus.requireOpen()
  requireChannel(channel)
  if not bus.channels.hasKey(channel):
    return 0
  var snapshot = newSeqOfCap[MemorySubscription](bus.channels[channel].len)
  for subscription in bus.channels[channel]:
    snapshot.add(subscription)
  for subscription in snapshot:
    if subscription.active:
      if subscription.queue.len < bus.options.pubSubMaxPendingMessages:
        subscription.queue.addLast(payload)
      subscription.startWorker()
      inc result

method onStateChange*(bus: InProcessPubSub, handler: StateHandler) {.gcsafe.} =
  if bus.isClosed:
    return
  bus.currentStateHandler = handler
  bus.notifyState(csConnected)

proc closeOwned(bus: InProcessPubSub) {.async.} =
  for subscription in bus.subscriptions:
    subscription.active = false
  for subscription in bus.retiring:
    subscription.active = false
  let owned = bus.subscriptions & bus.retiring
  for subscription in owned:
    if subscription.worker != nil and not subscription.worker.finished:
      await subscription.worker.cancelAndWait()
  if bus.currentStateHandler != nil:
    bus.notifyState(csClosed)
  if bus.stateWorker != nil and not bus.stateWorker.finished:
    try:
      await bus.stateWorker.wait(bus.options.operationTimeout)
    except AsyncTimeoutError:
      await bus.stateWorker.cancelAndWait()
  bus.channels.clear()
  bus.subscriptions.setLen(0)
  bus.retiring.setLen(0)
  bus.stateQueue.clear()
  bus.currentStateHandler = nil

proc joinClose(bus: InProcessPubSub): Future[void] {.async.} =
  if bus.closeTask == nil:
    bus.isClosed = true
    bus.closeTask = bus.closeOwned()
  await bus.closeTask.noCancel()

method close*(bus: InProcessPubSub): Future[void] =
  bus.joinClose()
