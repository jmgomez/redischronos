import std/[deques, options, random, sequtils, tables]
import chronos

import ./api
import ./errors
import ./options as backendoptions
import ./redisconnection
import ./redisurl
import ./resp2

type
  RedisSubscription = ref object of Subscription
    id: uint64
    channel: string
    handler: MessageHandler
    active: bool
    queue: Deque[string]
    worker: Future[void]
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
    subscribeAcks: Table[string, Future[void]]
    unsubscribeAcks: Table[string, Future[void]]
    liveChannels: Table[string, bool]
    nextId: uint64
    isClosed: bool
    connected: bool
    currentStateHandler: StateHandler
    stateQueue: Deque[ConnectionState]
    stateWorker: Future[void]
    rng: Rand

proc writeSubscriber(bus: RedisPubSub,
    arguments: openArray[string],
    deadline: Moment): Future[void] {.gcsafe.}

proc remaining(deadline: Moment): Duration =
  let now = Moment.now()
  if deadline <= now: 0.nanoseconds else: deadline - now

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
  let addresses = resolveTAddress(config.host, Port(config.port))
  if addresses.len == 0:
    raise newException(BackendConnectionError, "Redis host resolution failed")
  let connecting = connect(addresses[0])
  var transport: StreamTransport
  try:
    transport = await connecting.wait(options.connectTimeout)
  except AsyncTimeoutError:
    await connecting.cancelAndWait()
    raise newException(BackendTimeoutError, "Redis connect timed out")
  except TransportError:
    raise newException(BackendConnectionError, "Redis connect failed")

  let parser = newRespParser()
  try:
    if config.password.isSome:
      let reply =
        if config.username.isSome:
          await transport.boundedHandshake(parser, [
            "AUTH", config.username.get, config.password.get
          ], options.operationTimeout)
        else:
          await transport.boundedHandshake(
            parser,
            ["AUTH", config.password.get],
            options.operationTimeout
          )
      requireSimple(reply, "OK", authentication = true)
    if config.database != 0:
      requireSimple(
        await transport.boundedHandshake(
          parser,
          ["SELECT", $config.database],
          options.operationTimeout
        ),
        "OK"
      )
    requireSimple(
      await transport.boundedHandshake(
        parser,
        ["PING"],
        options.operationTimeout
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
        await bus.currentStateHandler(state)
      except CancelledError:
        raise
      except CatchableError:
        discard

proc notifyState(bus: RedisPubSub, state: ConnectionState) =
  bus.stateQueue.addLast(state)
  if bus.stateWorker == nil or bus.stateWorker.finished:
    bus.stateWorker = bus.observeStates()

proc deliver(subscription: RedisSubscription) {.async.} =
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

proc dispatch(bus: RedisPubSub, channel, payload: string) =
  if not bus.channels.hasKey(channel):
    return
  let snapshot = bus.channels[channel]
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

proc desiredChannels(bus: RedisPubSub): seq[string] =
  for channel in bus.channels.keys:
    if bus.hasDesiredChannel(channel):
      result.add(channel)

proc waitForChannelState(bus: RedisPubSub, transport: StreamTransport,
    parser: RespParser, channel: string, subscribed: bool) {.async.} =
  let operation = proc() {.async.} =
    while bus.liveChannels.hasKey(channel) != subscribed:
      let values = parser.feed(await transport.readSome())
      for value in values:
        bus.handleFrame(value)
  let waiting = operation()
  try:
    await waiting.wait(bus.options.operationTimeout)
  except AsyncTimeoutError:
    await waiting.cancelAndWait()
    raise newException(
      BackendTimeoutError,
      "Redis subscription acknowledgement timed out"
    )

proc writeCandidateImpl(bus: RedisPubSub, transport: StreamTransport,
    arguments: seq[string]) {.async.} =
  let writing = transport.write(encodeCommand(arguments))
  try:
    discard await writing.wait(bus.options.operationTimeout)
  except AsyncTimeoutError:
    await writing.cancelAndWait()
    raise newException(BackendTimeoutError, "Redis subscriber write timed out")

proc writeCandidate(bus: RedisPubSub, transport: StreamTransport,
    arguments: openArray[string]): Future[void] =
  bus.writeCandidateImpl(transport, @arguments)

proc reconcileCandidate(bus: RedisPubSub, transport: StreamTransport,
    parser: RespParser) {.async.} =
  while true:
    let desired = bus.desiredChannels()
    for channel in desired:
      if not bus.liveChannels.hasKey(channel):
        await bus.writeCandidate(transport, ["SUBSCRIBE", channel])
        await bus.waitForChannelState(transport, parser, channel, true)

    var stale: seq[string]
    for channel in bus.liveChannels.keys:
      if channel notin bus.desiredChannels():
        stale.add(channel)
    for channel in stale:
      await bus.writeCandidate(transport, ["UNSUBSCRIBE", channel])
      await bus.waitForChannelState(transport, parser, channel, false)

    let currentDesired = bus.desiredChannels()
    var converged = currentDesired.len == bus.liveChannels.len
    if converged:
      for channel in currentDesired:
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
  let acquiring = bus.writeLock.acquire()
  var acquired = false
  try:
    let budget = deadline.remaining()
    if budget <= 0.nanoseconds:
      raise newException(BackendTimeoutError, "Redis subscriber write timed out")
    await acquiring.wait(budget)
    acquired = true
  except AsyncTimeoutError:
    await acquiring.cancelAndWait()
    raise newException(BackendTimeoutError, "Redis subscriber write timed out")
  try:
    let writing = bus.subscriber.write(encodeCommand(arguments))
    try:
      let budget = deadline.remaining()
      if budget <= 0.nanoseconds:
        raise newException(
          BackendTimeoutError,
          "Redis subscriber write timed out"
        )
      discard await writing.wait(budget)
    except AsyncTimeoutError:
      await writing.cancelAndWait()
      raise newException(
        BackendTimeoutError,
        "Redis subscriber write timed out"
      )
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
    let acknowledgement = newFuture[void]("RedisPubSub.subscribe")
    bus.subscribeAcks[channel] = acknowledgement
    let deadline = Moment.now() + bus.options.operationTimeout
    try:
      await bus.writeSubscriber(["SUBSCRIBE", channel], deadline)
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
    except CancelledError:
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
    let acknowledgement = newFuture[void]("RedisPubSub.unsubscribe")
    bus.unsubscribeAcks[redisSubscription.channel] = acknowledgement
    let deadline = Moment.now() + bus.options.operationTimeout
    try:
      await bus.writeSubscriber(
        ["UNSUBSCRIBE", redisSubscription.channel],
        deadline
      )
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
    except CancelledError:
      if bus.unsubscribeAcks.hasKey(redisSubscription.channel):
        bus.unsubscribeAcks.del(redisSubscription.channel)
      if bus.subscriber != nil:
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
  bus.currentStateHandler = handler
  let state =
    if bus.isClosed: csClosed
    elif bus.connected: csConnected
    else: csDisconnected
  bus.notifyState(state)

method close*(bus: RedisPubSub): Future[void] {.async.} =
  if bus.isClosed:
    return
  bus.isClosed = true
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
    if subscription.worker != nil and not subscription.worker.finished:
      await subscription.worker.cancelAndWait()
  if bus.reader != nil and not bus.reader.finished:
    await bus.reader.cancelAndWait()
  if bus.subscriber != nil:
    await bus.subscriber.closeWait()
  await bus.publishConnection.close()
  bus.notifyState(csClosed)
  if bus.stateWorker != nil and not bus.stateWorker.finished:
    try:
      await bus.stateWorker.wait(bus.options.operationTimeout)
    except AsyncTimeoutError:
      await bus.stateWorker.cancelAndWait()

when defined(test):
  proc disconnectSubscriberForTest*(bus: PubSub) {.async.} =
    let redisBus = RedisPubSub(bus)
    if redisBus.subscriber != nil:
      await redisBus.subscriber.closeWait()
