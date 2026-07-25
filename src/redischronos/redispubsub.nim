import std/[options, random, tables]
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
    queue: seq[string]
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
    stateTask: Future[void]

proc writeSubscriber(bus: RedisPubSub,
    arguments: openArray[string]): Future[void] {.gcsafe.}

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

proc notifyState(bus: RedisPubSub,
    state: ConnectionState): Future[void] {.async.} =
  if bus.currentStateHandler != nil:
    try:
      await bus.currentStateHandler(state)
    except CancelledError:
      raise
    except CatchableError:
      discard

proc deliver(subscription: RedisSubscription) {.async.} =
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

proc dispatch(bus: RedisPubSub, channel, payload: string) =
  if not bus.channels.hasKey(channel):
    return
  let snapshot = bus.channels[channel]
  for subscription in snapshot:
    if subscription.active:
      subscription.queue.add(payload)
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

proc resubscribe(bus: RedisPubSub) {.async.} =
  while true:
    var changed = false
    for channel in bus.channels.keys:
      if bus.hasDesiredChannel(channel) and
          not bus.liveChannels.hasKey(channel):
        await bus.writeSubscriber(["SUBSCRIBE", channel])
        bus.handleFrame(await bus.subscriber.readOne(bus.parser))
        changed = true
    if not changed:
      break

proc reconnect(bus: RedisPubSub) {.async.} =
  var attempt = 0
  while not bus.isClosed:
    let baseMilliseconds = min(1000, 50 * (1 shl min(attempt, 4)))
    let jitterMilliseconds = rand(max(1, baseMilliseconds div 4))
    await sleepAsync((baseMilliseconds + jitterMilliseconds).milliseconds)
    if bus.isClosed:
      return
    try:
      let (subscriber, parser) =
        await connectSubscriber(bus.config, bus.options)
      bus.subscriber = subscriber
      bus.parser = parser
      bus.liveChannels.clear()
      await bus.resubscribe()
      bus.connected = true
      await bus.notifyState(csConnected)
      return
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
      await bus.notifyState(csDisconnected)
      await bus.reconnect()

proc newRedisPubSub*(url: string,
    options = defaultBackendOptions()): Future[PubSub] {.async.} =
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
    connected: true
  )
  bus.reader = bus.readerLoop()
  return bus

proc requireOpen(bus: RedisPubSub) =
  if bus.isClosed:
    raise newException(BackendClosedError, "pub/sub backend is closed")

proc writeSubscriberImpl(bus: RedisPubSub,
    arguments: seq[string]) {.async.} =
  let acquiring = bus.writeLock.acquire()
  try:
    await acquiring.wait(bus.options.operationTimeout)
  except AsyncTimeoutError:
    await acquiring.cancelAndWait()
    raise newException(BackendTimeoutError, "Redis subscriber write timed out")
  try:
    discard await bus.subscriber.write(encodeCommand(arguments))
  finally:
    bus.writeLock.release()

proc writeSubscriber(bus: RedisPubSub,
    arguments: openArray[string]): Future[void] {.gcsafe.} =
  bus.writeSubscriberImpl(@arguments)

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
    await bus.writeSubscriber(["SUBSCRIBE", channel])
    try:
      await acknowledgement.wait(bus.options.operationTimeout)
    except AsyncTimeoutError:
      raise newException(BackendTimeoutError, "Redis subscribe timed out")
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
  var remaining = false
  for item in bus.channels[redisSubscription.channel]:
    if item.active:
      remaining = true
  if not remaining:
    if not bus.connected:
      return
    let acknowledgement = newFuture[void]("RedisPubSub.unsubscribe")
    bus.unsubscribeAcks[redisSubscription.channel] = acknowledgement
    await bus.writeSubscriber(["UNSUBSCRIBE", redisSubscription.channel])
    try:
      await acknowledgement.wait(bus.options.operationTimeout)
    except AsyncTimeoutError:
      raise newException(BackendTimeoutError, "Redis unsubscribe timed out")

method publish*(bus: RedisPubSub, channel,
    payload: string): Future[int64] {.async.} =
  bus.requireOpen()
  if channel.len == 0:
    raise newException(InvalidArgumentError, "channel must not be empty")
  let reply =
    await bus.publishConnection.execute(["PUBLISH", channel, payload])
  if reply.kind != rkInteger:
    raise newException(ProtocolError, "unexpected Redis PUBLISH reply")
  return reply.integer

method onStateChange*(bus: RedisPubSub,
    handler: StateHandler) {.gcsafe.} =
  bus.currentStateHandler = handler
  let state =
    if bus.isClosed: csClosed
    elif bus.connected: csConnected
    else: csDisconnected
  bus.stateTask = bus.notifyState(state)

method close*(bus: RedisPubSub): Future[void] {.async.} =
  if bus.isClosed:
    return
  bus.isClosed = true
  for subscription in bus.subscriptions:
    subscription.active = false
    if subscription.worker != nil and not subscription.worker.finished:
      await subscription.worker.cancelAndWait()
  if bus.reader != nil and not bus.reader.finished:
    await bus.reader.cancelAndWait()
  if bus.subscriber != nil:
    await bus.subscriber.closeWait()
  await bus.publishConnection.close()
  if bus.stateTask != nil and not bus.stateTask.finished:
    await bus.stateTask
  await bus.notifyState(csClosed)

when defined(test):
  proc disconnectSubscriberForTest*(bus: PubSub) {.async.} =
    let redisBus = RedisPubSub(bus)
    if redisBus.subscriber != nil:
      await redisBus.subscriber.closeWait()
