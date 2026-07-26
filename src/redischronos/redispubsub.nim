import std/[deques, options, random, sequtils, sets, tables]
import chronos

import ./api
import ./errors
import ./options as backendoptions
import ./redisconnection
import ./redisresolve
import ./redisurl
import ./resp2

type
  SubscriberAdmissionTimeoutError = object of CatchableError

  RedisSubscription = ref object of Subscription
    id: uint64
    channel: string
    handler: MessageHandler
    active: bool
    queue: Deque[string]
    worker: Future[void]
    handlerTask: Future[void]
    owner: RedisPubSub

  RedisPubSub* = ref object of PubSub
    config: RedisConfig
    options: BackendOptions
    publishConnection: RedisConnection
    subscriber: StreamTransport
    parser: RespParser
    reader: Future[void]
    writeLock: AsyncLock
    channels: Table[string, seq[RedisSubscription]]
    subscriptions: seq[RedisSubscription]
    retiring: seq[RedisSubscription]
    subscribeAcks: Table[string, Future[void]]
    unsubscribeAcks: Table[string, Future[void]]
    liveChannels: Table[string, bool]
    nextId: uint64
    isClosed: bool
    connected: bool
    currentStateHandler: StateHandler
    stateQueue: Deque[ConnectionState]
    stateWorker: Future[void]
    stateHandlerTask: Future[void]
    terminalObserverActive: bool
    rng: Rand
    closeTask: Future[void]
    closeCallers: seq[Future[void]]
    when defined(test):
      reconciliationChecks: int

proc writeSubscriber(bus: RedisPubSub,
    arguments: openArray[string],
    deadline: Moment): Future[void] {.gcsafe.}

proc remaining(deadline: Moment): Duration =
  let now = Moment.now()
  if deadline <= now: 0.nanoseconds else: deadline - now

proc containsFuture(root, target: FutureBase): bool =
  var current = root
  var depth = 0
  while current != nil and depth < 1024:
    if current == target:
      return true
    current = current.internalChild
    inc depth

proc activeCloseCaller(bus: RedisPubSub,
    root: FutureBase): Future[void] =
  for caller in bus.closeCallers:
    if not caller.finished and containsFuture(root, caller):
      return caller

proc removeCloseCaller(bus: RedisPubSub, caller: Future[void]) =
  bus.closeCallers.keepItIf(it != caller)

proc bytesToString(bytes: seq[byte]): string =
  result = newString(bytes.len)
  for index, value in bytes:
    result[index] = char(value)

proc readSome(transport: StreamTransport): Future[string] {.async.} =
  var bytes = newSeq[byte](4096)
  let count = await transport.readOnce(addr bytes[0], bytes.len)
  if count == 0:
    raise newException(BackendConnectionError, "Redis subscriber closed")
  bytes.setLen(count)
  return bytesToString(bytes)

proc readOne(transport: StreamTransport,
    parser: RespParser): Future[RespValue] {.async.} =
  while true:
    let values = parser.feed(await transport.readSome())
    if values.len > 0:
      return values[0]

proc sendHandshakeImpl(transport: StreamTransport, parser: RespParser,
    arguments: seq[string]): Future[RespValue] {.async.} =
  discard await transport.write(encodeCommand(arguments))
  return await transport.readOne(parser)

proc sendHandshake(transport: StreamTransport, parser: RespParser,
    arguments: openArray[string]): Future[RespValue] =
  transport.sendHandshakeImpl(parser, @arguments)

proc boundedHandshakeImpl(transport: StreamTransport, parser: RespParser,
    arguments: seq[string],
    timeout: Duration): Future[RespValue] {.async.} =
  if timeout <= 0.nanoseconds:
    raise newException(BackendTimeoutError, "Redis handshake timed out")
  let operation = transport.sendHandshake(parser, arguments)
  try:
    return await operation.wait(timeout)
  except AsyncTimeoutError:
    await operation.cancelAndWait()
    raise newException(BackendTimeoutError, "Redis handshake timed out")

proc boundedHandshake(transport: StreamTransport, parser: RespParser,
    arguments: openArray[string],
    timeout: Duration): Future[RespValue] =
  transport.boundedHandshakeImpl(parser, @arguments, timeout)

proc requireSimple(reply: RespValue, expected: string,
    authentication = false) =
  if reply.kind == rkError:
    if authentication:
      raise newException(
        RedisAuthenticationError,
        "Redis authentication failed"
      )
    raise newException(RedisCommandError, "Redis handshake failed")
  if reply.kind != rkSimpleString or reply.text != expected:
    raise newException(ProtocolError, "unexpected Redis handshake reply")

proc connectSubscriber(config: RedisConfig,
    options: BackendOptions): Future[(StreamTransport, RespParser)] {.async.} =
  let establishmentDeadline = Moment.now() + options.connectTimeout
  let addresses = await resolveRedisAddresses(
    config.host, config.port, establishmentDeadline.remaining()
  )
  var transport: StreamTransport
  for address in addresses:
    let budget = establishmentDeadline.remaining()
    if budget <= 0.nanoseconds:
      raise newException(BackendTimeoutError, "Redis connect timed out")
    let connecting = connect(address)
    try:
      transport = await connecting.wait(budget)
      break
    except AsyncTimeoutError:
      await connecting.cancelAndWait()
    except CancelledError:
      await connecting.cancelAndWait()
      raise
    except TransportError:
      discard
  if transport == nil:
    if establishmentDeadline.remaining() <= 0.nanoseconds:
      raise newException(BackendTimeoutError, "Redis connect timed out")
    raise newException(BackendConnectionError, "Redis connect failed")

  let parser = newRespParser()
  let handshakeDeadline = establishmentDeadline
  try:
    if config.password.isSome:
      let reply =
        if config.username.isSome:
          await transport.boundedHandshake(parser, [
            "AUTH", config.username.get, config.password.get
          ], handshakeDeadline.remaining())
        else:
          await transport.boundedHandshake(
            parser,
            ["AUTH", config.password.get],
            handshakeDeadline.remaining()
          )
      requireSimple(reply, "OK", authentication = true)
    if config.database != 0:
      requireSimple(
        await transport.boundedHandshake(
          parser,
          ["SELECT", $config.database],
          handshakeDeadline.remaining()
        ),
        "OK"
      )
    requireSimple(
      await transport.boundedHandshake(
        parser,
        ["PING"],
        handshakeDeadline.remaining()
      ),
      "PONG"
    )
  except CatchableError:
    await transport.closeWait()
    raise
  return (transport, parser)

proc observeStates(bus: RedisPubSub) {.async.} =
  while bus.stateQueue.len > 0:
    let state = bus.stateQueue.popFirst()
    if bus.currentStateHandler != nil:
      try:
        bus.terminalObserverActive = state == csClosed
        bus.stateHandlerTask = bus.currentStateHandler(state)
        await bus.stateHandlerTask
      except CancelledError:
        raise
      except CatchableError:
        discard
      finally:
        bus.stateHandlerTask = nil
        bus.terminalObserverActive = false

proc notifyState(bus: RedisPubSub, state: ConnectionState) =
  bus.stateQueue.addLast(state)
  if bus.stateWorker == nil or bus.stateWorker.finished:
    bus.stateWorker = bus.observeStates()

proc deliver(subscription: RedisSubscription) {.async.} =
  try:
    while subscription.active and subscription.queue.len > 0:
      let payload = subscription.queue.popFirst()
      if not subscription.active:
        break
      try:
        subscription.handlerTask =
          subscription.handler(subscription.channel, payload)
        await subscription.handlerTask
      except CancelledError:
        raise
      except CatchableError:
        if subscription.owner.options.onHandlerError != nil:
          subscription.owner.options.onHandlerError(HandlerError(
            channel: subscription.channel,
            subscriptionId: subscription.id,
            cause: newException(ValueError, "message handler failed")
          ))
      finally:
        subscription.handlerTask = nil
  finally:
    if not subscription.active:
      subscription.queue.clear()
      subscription.handler = nil
      subscription.owner.retiring.keepItIf(it != subscription)

proc dispatch(bus: RedisPubSub, channel, payload: string) =
  if not bus.channels.hasKey(channel):
    return
  var snapshot = newSeqOfCap[RedisSubscription](bus.channels[channel].len)
  for subscription in bus.channels[channel]:
    snapshot.add(subscription)
  for subscription in snapshot:
    if subscription.active:
      if subscription.queue.len <
          bus.options.pubSubMaxPendingMessages:
        subscription.queue.addLast(payload)
      if subscription.worker == nil or subscription.worker.finished:
        subscription.worker = deliver(subscription)

proc arrayText(value: RespValue, index: int): string =
  if value.kind != rkArray or index >= value.items.len or
      value.items[index].kind != rkBulkString:
    raise newException(ProtocolError, "malformed Redis Pub/Sub frame")
  value.items[index].text

proc handleFrame(bus: RedisPubSub, value: RespValue) =
  let kind = arrayText(value, 0)
  case kind
  of "message":
    if value.items.len != 3:
      raise newException(ProtocolError, "malformed Redis message frame")
    bus.dispatch(arrayText(value, 1), arrayText(value, 2))
  of "subscribe":
    let channel = arrayText(value, 1)
    bus.liveChannels[channel] = true
    if bus.subscribeAcks.hasKey(channel):
      let acknowledgement = bus.subscribeAcks[channel]
      bus.subscribeAcks.del(channel)
      if not acknowledgement.finished:
        acknowledgement.complete()
  of "unsubscribe":
    let channel = arrayText(value, 1)
    bus.liveChannels.del(channel)
    if bus.unsubscribeAcks.hasKey(channel):
      let acknowledgement = bus.unsubscribeAcks[channel]
      bus.unsubscribeAcks.del(channel)
      if not acknowledgement.finished:
        acknowledgement.complete()
  else:
    raise newException(ProtocolError, "unexpected Redis Pub/Sub frame")

proc hasDesiredChannel(bus: RedisPubSub, channel: string): bool =
  if not bus.channels.hasKey(channel):
    return false
  for subscription in bus.channels[channel]:
    if subscription.active:
      return true

proc desiredChannels(bus: RedisPubSub): HashSet[string] =
  result = initHashSet[string]()
  for channel in bus.channels.keys:
    when defined(test):
      inc bus.reconciliationChecks
    if bus.hasDesiredChannel(channel):
      result.incl(channel)

proc waitForChannelState(bus: RedisPubSub, transport: StreamTransport,
    parser: RespParser, channel: string, subscribed: bool,
    deadline: Moment) {.async.} =
  let budget = deadline.remaining()
  if budget <= 0.nanoseconds:
    raise newException(
      BackendTimeoutError,
      "Redis subscription acknowledgement timed out"
    )
  let operation = proc() {.async.} =
    while bus.liveChannels.hasKey(channel) != subscribed:
      let values = parser.feed(await transport.readSome())
      for value in values:
        bus.handleFrame(value)
  let waiting = operation()
  try:
    await waiting.wait(budget)
  except AsyncTimeoutError:
    await waiting.cancelAndWait()
    raise newException(
      BackendTimeoutError,
      "Redis subscription acknowledgement timed out"
    )

proc writeCandidateImpl(bus: RedisPubSub, transport: StreamTransport,
    arguments: seq[string], deadline: Moment) {.async.} =
  let budget = deadline.remaining()
  if budget <= 0.nanoseconds:
    raise newException(BackendTimeoutError, "Redis subscriber write timed out")
  let writing = transport.write(encodeCommand(arguments))
  try:
    discard await writing.wait(budget)
  except AsyncTimeoutError:
    await writing.cancelAndWait()
    raise newException(BackendTimeoutError, "Redis subscriber write timed out")

proc writeCandidate(bus: RedisPubSub, transport: StreamTransport,
    arguments: openArray[string], deadline: Moment): Future[void] =
  bus.writeCandidateImpl(transport, @arguments, deadline)

proc reconcileCandidate(bus: RedisPubSub, transport: StreamTransport,
    parser: RespParser) {.async.} =
  let deadline = Moment.now() + bus.options.operationTimeout
  while true:
    let desired = bus.desiredChannels()
    for channel in desired:
      when defined(test):
        inc bus.reconciliationChecks
      if not bus.liveChannels.hasKey(channel):
        await bus.writeCandidate(transport, ["SUBSCRIBE", channel], deadline)
        await bus.waitForChannelState(
          transport, parser, channel, true, deadline
        )

    var stale: seq[string]
    for channel in bus.liveChannels.keys:
      when defined(test):
        inc bus.reconciliationChecks
      if channel notin desired:
        stale.add(channel)
    for channel in stale:
      await bus.writeCandidate(transport, ["UNSUBSCRIBE", channel], deadline)
      await bus.waitForChannelState(
        transport, parser, channel, false, deadline
      )

    let currentDesired = bus.desiredChannels()
    var converged = currentDesired.len == bus.liveChannels.len
    if converged:
      for channel in currentDesired:
        when defined(test):
          inc bus.reconciliationChecks
        if not bus.liveChannels.hasKey(channel):
          converged = false
    if converged:
      return

proc reconnect(bus: RedisPubSub) {.async.} =
  var attempt = 0
  while not bus.isClosed:
    let baseMilliseconds = min(1000, 50 * (1 shl min(attempt, 4)))
    let jitterCap = max(1, baseMilliseconds div 4)
    let jitterMilliseconds =
      if bus.options.reconnectJitterSource != nil:
        clamp(bus.options.reconnectJitterSource(jitterCap), 0, jitterCap)
      else:
        bus.rng.rand(jitterCap)
    await sleepAsync((baseMilliseconds + jitterMilliseconds).milliseconds)
    if bus.isClosed:
      return
    try:
      let (candidate, candidateParser) =
        await connectSubscriber(bus.config, bus.options)
      try:
        bus.liveChannels.clear()
        await bus.reconcileCandidate(candidate, candidateParser)
        bus.subscriber = candidate
        bus.parser = candidateParser
        bus.connected = true
        bus.notifyState(csConnected)
        return
      except CancelledError:
        candidate.close()
        await candidate.closeWait()
        raise
      except CatchableError:
        candidate.close()
        await candidate.closeWait()
        bus.liveChannels.clear()
        raise
    except CancelledError:
      raise
    except CatchableError:
      inc attempt

proc readerLoop(bus: RedisPubSub) {.async.} =
  while not bus.isClosed:
    try:
      let values = bus.parser.feed(await bus.subscriber.readSome())
      for value in values:
        bus.handleFrame(value)
    except CancelledError:
      raise
    except CatchableError as error:
      if bus.isClosed:
        return
      bus.connected = false
      bus.liveChannels.clear()
      for _, acknowledgement in bus.subscribeAcks.pairs:
        if not acknowledgement.finished:
          acknowledgement.fail(error)
      for _, acknowledgement in bus.unsubscribeAcks.pairs:
        if not acknowledgement.finished:
          acknowledgement.fail(error)
      if bus.subscriber != nil:
        await bus.subscriber.closeWait()
      bus.notifyState(csDisconnected)
      await bus.reconnect()

proc newRedisPubSub*(url: string,
    options = defaultBackendOptions()): Future[PubSub] {.async.} =
  if options.pubSubMaxPendingMessages <= 0:
    raise newException(
      InvalidArgumentError,
      "pubSubMaxPendingMessages must be positive"
    )
  let config = parseRedisUrl(url)
  let (subscriber, parser) = await connectSubscriber(config, options)
  let bus = RedisPubSub(
    config: config,
    options: options,
    publishConnection: newRedisConnection(config, options),
    subscriber: subscriber,
    parser: parser,
    writeLock: newAsyncLock(),
    channels: initTable[string, seq[RedisSubscription]](),
    subscribeAcks: initTable[string, Future[void]](),
    unsubscribeAcks: initTable[string, Future[void]](),
    liveChannels: initTable[string, bool](),
    connected: true,
    rng: initRand()
  )
  bus.reader = bus.readerLoop()
  return bus

proc requireOpen(bus: RedisPubSub) =
  if bus.isClosed:
    raise newException(BackendClosedError, "pub/sub backend is closed")

proc writeSubscriberImpl(bus: RedisPubSub,
    arguments: seq[string], deadline: Moment) {.async.} =
  let admissionBudget = deadline.remaining()
  if admissionBudget <= 0.nanoseconds:
    raise newException(BackendTimeoutError, "Redis subscriber write timed out")
  let acquiring = bus.writeLock.acquire()
  var acquired = false
  try:
    await acquiring.wait(admissionBudget)
    acquired = true
  except AsyncTimeoutError:
    await acquiring.cancelAndWait()
    raise newException(
      SubscriberAdmissionTimeoutError,
      "Redis subscriber lock timed out"
    )
  try:
    let budget = deadline.remaining()
    if budget <= 0.nanoseconds:
      raise newException(
        BackendTimeoutError,
        "Redis subscriber write timed out"
      )
    let writing = bus.subscriber.write(encodeCommand(arguments))
    try:
      discard await writing.wait(budget)
    except AsyncTimeoutError:
      await writing.cancelAndWait()
      if bus.subscriber != nil:
        bus.subscriber.close()
      raise newException(
        BackendTimeoutError,
        "Redis subscriber write timed out"
      )
    except CancelledError:
      await writing.cancelAndWait()
      if bus.subscriber != nil:
        bus.subscriber.close()
      raise
  finally:
    if acquired:
      bus.writeLock.release()

proc writeSubscriber(bus: RedisPubSub,
    arguments: openArray[string],
    deadline: Moment): Future[void] {.gcsafe.} =
  bus.writeSubscriberImpl(@arguments, deadline)

method subscribe*(bus: RedisPubSub, channel: string,
    handler: MessageHandler): Future[Subscription] {.async.} =
  bus.requireOpen()
  if channel.len == 0 or handler == nil:
    raise newException(InvalidArgumentError, "channel and handler are required")
  var first = true
  if bus.channels.hasKey(channel):
    for item in bus.channels[channel]:
      if item.active:
        first = false
  inc bus.nextId
  let subscription = RedisSubscription(
    id: bus.nextId, channel: channel, handler: handler,
    active: true, owner: bus
  )
  bus.channels.mgetOrPut(channel, @[]).add(subscription)
  bus.subscriptions.add(subscription)
  if first:
    if not bus.connected:
      return subscription
    if bus.options.operationTimeout <= 0.nanoseconds:
      subscription.active = false
      bus.subscriptions.keepItIf(it != subscription)
      bus.channels[channel].keepItIf(it != subscription)
      if bus.channels[channel].len == 0:
        bus.channels.del(channel)
      raise newException(BackendTimeoutError, "Redis subscribe timed out")
    let acknowledgement = newFuture[void]("RedisPubSub.subscribe")
    bus.subscribeAcks[channel] = acknowledgement
    let deadline = Moment.now() + bus.options.operationTimeout
    var written = false
    try:
      await bus.writeSubscriber(["SUBSCRIBE", channel], deadline)
      written = true
      let budget = deadline.remaining()
      if budget <= 0.nanoseconds:
        raise newException(BackendTimeoutError, "Redis subscribe timed out")
      await acknowledgement.wait(budget)
    except AsyncTimeoutError:
      if bus.subscribeAcks.hasKey(channel):
        bus.subscribeAcks.del(channel)
      subscription.active = false
      subscription.queue.clear()
      bus.subscriptions.keepItIf(it != subscription)
      bus.channels[channel].keepItIf(it != subscription)
      if bus.channels[channel].len == 0:
        bus.channels.del(channel)
      if bus.subscriber != nil:
        bus.subscriber.close()
      raise newException(BackendTimeoutError, "Redis subscribe timed out")
    except SubscriberAdmissionTimeoutError:
      if bus.subscribeAcks.hasKey(channel):
        bus.subscribeAcks.del(channel)
      subscription.active = false
      bus.subscriptions.keepItIf(it != subscription)
      bus.channels[channel].keepItIf(it != subscription)
      if bus.channels[channel].len == 0:
        bus.channels.del(channel)
      raise newException(BackendTimeoutError, "Redis subscribe timed out")
    except CancelledError:
      if bus.subscribeAcks.hasKey(channel):
        bus.subscribeAcks.del(channel)
      subscription.active = false
      subscription.queue.clear()
      bus.subscriptions.keepItIf(it != subscription)
      bus.channels[channel].keepItIf(it != subscription)
      if bus.channels[channel].len == 0:
        bus.channels.del(channel)
      if written and bus.subscriber != nil:
        bus.subscriber.close()
      raise
    except CatchableError:
      if bus.subscribeAcks.hasKey(channel):
        bus.subscribeAcks.del(channel)
      subscription.active = false
      subscription.queue.clear()
      bus.subscriptions.keepItIf(it != subscription)
      bus.channels[channel].keepItIf(it != subscription)
      if bus.channels[channel].len == 0:
        bus.channels.del(channel)
      if bus.subscriber != nil:
        bus.subscriber.close()
      raise
  return subscription

method unsubscribe*(bus: RedisPubSub,
    subscription: Subscription): Future[void] {.async.} =
  bus.requireOpen()
  if subscription == nil or not (subscription of RedisSubscription):
    return
  let redisSubscription = RedisSubscription(subscription)
  if redisSubscription.owner != bus or not redisSubscription.active:
    return
  redisSubscription.active = false
  redisSubscription.queue.clear()
  if redisSubscription.worker != nil and
      not redisSubscription.worker.finished:
    bus.retiring.add(redisSubscription)
  var remaining = false
  for item in bus.channels[redisSubscription.channel]:
    if item.active:
      remaining = true
  bus.subscriptions.keepItIf(it != redisSubscription)
  if bus.channels.hasKey(redisSubscription.channel):
    bus.channels[redisSubscription.channel].keepItIf(
      it != redisSubscription
    )
    if bus.channels[redisSubscription.channel].len == 0:
      bus.channels.del(redisSubscription.channel)
  if not remaining:
    if not bus.connected:
      return
    if bus.options.operationTimeout <= 0.nanoseconds:
      redisSubscription.active = true
      bus.retiring.keepItIf(it != redisSubscription)
      bus.channels.mgetOrPut(
        redisSubscription.channel, @[]
      ).add(redisSubscription)
      bus.subscriptions.add(redisSubscription)
      raise newException(BackendTimeoutError, "Redis unsubscribe timed out")
    let acknowledgement = newFuture[void]("RedisPubSub.unsubscribe")
    bus.unsubscribeAcks[redisSubscription.channel] = acknowledgement
    let deadline = Moment.now() + bus.options.operationTimeout
    var written = false
    try:
      await bus.writeSubscriber(
        ["UNSUBSCRIBE", redisSubscription.channel],
        deadline
      )
      written = true
      let budget = deadline.remaining()
      if budget <= 0.nanoseconds:
        raise newException(BackendTimeoutError, "Redis unsubscribe timed out")
      await acknowledgement.wait(budget)
    except AsyncTimeoutError:
      if bus.unsubscribeAcks.hasKey(redisSubscription.channel):
        bus.unsubscribeAcks.del(redisSubscription.channel)
      if bus.subscriber != nil:
        bus.subscriber.close()
      raise newException(BackendTimeoutError, "Redis unsubscribe timed out")
    except SubscriberAdmissionTimeoutError:
      if bus.unsubscribeAcks.hasKey(redisSubscription.channel):
        bus.unsubscribeAcks.del(redisSubscription.channel)
      raise newException(BackendTimeoutError, "Redis unsubscribe timed out")
    except CancelledError:
      if bus.unsubscribeAcks.hasKey(redisSubscription.channel):
        bus.unsubscribeAcks.del(redisSubscription.channel)
      if written and bus.subscriber != nil:
        bus.subscriber.close()
      raise
    except CatchableError:
      if bus.unsubscribeAcks.hasKey(redisSubscription.channel):
        bus.unsubscribeAcks.del(redisSubscription.channel)
      if bus.subscriber != nil:
        bus.subscriber.close()
      raise

method publish*(bus: RedisPubSub, channel,
    payload: string): Future[int64] {.async.} =
  bus.requireOpen()
  if channel.len == 0:
    raise newException(InvalidArgumentError, "channel must not be empty")
  let reply =
    await bus.publishConnection.execute(["PUBLISH", channel, payload])
  if reply.kind != rkInteger:
    raise newException(ProtocolError, "unexpected Redis PUBLISH reply")
  if bus.channels.hasKey(channel):
    for subscription in bus.channels[channel]:
      if subscription.active:
        inc result

method onStateChange*(bus: RedisPubSub,
    handler: StateHandler) {.gcsafe.} =
  if bus.isClosed:
    return
  bus.currentStateHandler = handler
  let state =
    if bus.isClosed: csClosed
    elif bus.connected: csConnected
    else: csDisconnected
  bus.notifyState(state)

proc closeOwned(bus: RedisPubSub) {.async.} =
  await sleepAsync(0.milliseconds)
  let closedError =
    newException(BackendClosedError, "pub/sub backend is closed")
  for _, acknowledgement in bus.subscribeAcks.pairs:
    if not acknowledgement.finished:
      acknowledgement.fail(closedError)
  for _, acknowledgement in bus.unsubscribeAcks.pairs:
    if not acknowledgement.finished:
      acknowledgement.fail(closedError)
  bus.subscribeAcks.clear()
  bus.unsubscribeAcks.clear()
  for subscription in bus.subscriptions:
    subscription.active = false
  for subscription in bus.retiring:
    subscription.active = false
  let owned = bus.subscriptions & bus.retiring
  for subscription in owned:
    if subscription.worker != nil and not subscription.worker.finished:
      let caller = bus.activeCloseCaller(subscription.handlerTask)
      if caller != nil:
        caller.complete()
        await subscription.worker
      else:
        await subscription.worker.cancelAndWait()
  if bus.reader != nil and not bus.reader.finished:
    await bus.reader.cancelAndWait()
  if bus.subscriber != nil:
    await bus.subscriber.closeWait()
  await bus.publishConnection.close()
  bus.notifyState(csClosed)
  if bus.stateWorker != nil and not bus.stateWorker.finished:
    let caller = bus.activeCloseCaller(bus.stateHandlerTask)
    if caller != nil:
      caller.complete()
      await bus.stateWorker
    else:
      try:
        await bus.stateWorker.wait(bus.options.operationTimeout)
      except AsyncTimeoutError:
        await bus.stateWorker.cancelAndWait()
  bus.channels.clear()
  bus.subscriptions.setLen(0)
  bus.retiring.setLen(0)
  bus.liveChannels.clear()
  bus.stateQueue.clear()
  bus.currentStateHandler = nil

proc ensureClose(bus: RedisPubSub) =
  if bus.closeTask == nil:
    bus.isClosed = true
    bus.closeTask = bus.closeOwned()

method close*(bus: RedisPubSub): Future[void] =
  bus.ensureClose()
  if bus.terminalObserverActive:
    let handedOff = newFuture[void]("Redis terminal observer close handoff")
    handedOff.complete()
    return handedOff
  let caller = newFuture[void](
    "Redis Pub/Sub close caller",
    {FutureFlag.OwnCancelSchedule}
  )
  caller.cancelCallback = nil
  bus.closeCallers.add(caller)
  proc finishCaller(_: pointer) {.gcsafe, raises: [].} =
    if not caller.finished:
      if bus.closeTask.failed:
        caller.fail(bus.closeTask.error)
      elif bus.closeTask.cancelled:
        caller.cancelSoon()
      else:
        caller.complete()
  proc pruneCaller(_: pointer) {.gcsafe, raises: [].} =
    bus.removeCloseCaller(caller)
  bus.closeTask.addCallback(finishCaller, nil)
  caller.addCallback(pruneCaller, nil)
  result = caller

when defined(test):
  proc disconnectSubscriberForTest*(bus: PubSub) {.async.} =
    let redisBus = RedisPubSub(bus)
    if redisBus.subscriber != nil:
      await redisBus.subscriber.closeWait()

  proc holdSubscriberWriteLockForTest*(bus: PubSub,
      acquired, release: Future[void]) {.async.} =
    let redisBus = RedisPubSub(bus)
    await redisBus.writeLock.acquire()
    acquired.complete()
    try:
      await release
    finally:
      redisBus.writeLock.release()

  proc setOperationTimeoutForTest*(bus: PubSub, timeout: Duration) =
    RedisPubSub(bus).options.operationTimeout = timeout

  proc seedDesiredChannelsForTest*(bus: PubSub, count: int) =
    let redisBus = RedisPubSub(bus)
    for index in 0 ..< count:
      inc redisBus.nextId
      let subscription = RedisSubscription(
        id: redisBus.nextId,
        channel: "reconciliation:" & $index,
        active: true,
        owner: redisBus
      )
      redisBus.subscriptions.add(subscription)
      redisBus.channels.mgetOrPut(subscription.channel, @[]).add(subscription)
    redisBus.reconciliationChecks = 0

  proc reconciliationChecksForTest*(bus: PubSub): int =
    RedisPubSub(bus).reconciliationChecks

  proc deliveryStateCountsForTest*(bus: PubSub): (int, int, int) =
    let redisBus = RedisPubSub(bus)
    (
      redisBus.channels.len,
      redisBus.subscriptions.len,
      redisBus.retiring.len
    )

  proc closeCallerCountForTest*(bus: PubSub): int =
    RedisPubSub(bus).closeCallers.len
