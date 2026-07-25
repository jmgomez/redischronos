import std/[tables]
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
    queue: seq[string]
    worker: Future[void]
    owner: InProcessPubSub

  InProcessPubSub* = ref object of PubSub
    channels: Table[string, seq[MemorySubscription]]
    subscriptions: seq[MemorySubscription]
    nextId: uint64
    isClosed: bool
    options: BackendOptions
    currentStateHandler: StateHandler
    stateTask: Future[void]

proc newInProcessPubSub*(options: BackendOptions): PubSub =
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
  while subscription.queue.len > 0:
    let payload = subscription.queue[0]
    subscription.queue.delete(0)
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

proc notifyState(bus: InProcessPubSub,
    state: ConnectionState): Future[void] {.async.} =
  if bus.currentStateHandler != nil:
    try:
      await bus.currentStateHandler(state)
    except CancelledError:
      raise
    except CatchableError:
      discard

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
    if memorySubscription.owner == bus:
      memorySubscription.active = false

method publish*(bus: InProcessPubSub, channel,
    payload: string): Future[int64] {.async.} =
  bus.requireOpen()
  requireChannel(channel)
  if not bus.channels.hasKey(channel):
    return 0
  let snapshot = bus.channels[channel]
  for subscription in snapshot:
    if subscription.active:
      subscription.queue.add(payload)
      subscription.startWorker()
      inc result

method onStateChange*(bus: InProcessPubSub, handler: StateHandler) {.gcsafe.} =
  bus.currentStateHandler = handler
  let state = if bus.isClosed: csClosed else: csConnected
  bus.stateTask = bus.notifyState(state)

method close*(bus: InProcessPubSub): Future[void] {.async.} =
  if bus.isClosed:
    return
  bus.isClosed = true
  for subscription in bus.subscriptions:
    subscription.active = false
  for subscription in bus.subscriptions:
    if subscription.worker != nil and not subscription.worker.finished:
      await subscription.worker.cancelAndWait()
  if bus.stateTask != nil and not bus.stateTask.finished:
    await bus.stateTask
  if bus.currentStateHandler != nil:
    await bus.notifyState(csClosed)
  bus.channels.clear()
  bus.subscriptions.setLen(0)
