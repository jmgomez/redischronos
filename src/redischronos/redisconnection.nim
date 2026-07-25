import std/options
import chronos

import ./errors
import ./options as backendoptions
import ./redisurl
import ./resp2

type RedisConnection* = ref object
  config: RedisConfig
  options: BackendOptions
  lock: AsyncLock
  transport: StreamTransport
  parser: RespParser
  isClosed: bool

proc newRedisConnection*(config: RedisConfig,
    options = defaultBackendOptions()): RedisConnection =
  RedisConnection(
    config: config,
    options: options,
    lock: newAsyncLock(),
    parser: newRespParser()
  )

proc disconnect(connection: RedisConnection) {.async.} =
  if connection.transport != nil:
    let transport = connection.transport
    connection.transport = nil
    transport.close()
    await transport.closeWait()
  connection.parser = newRespParser()

proc bytesToString(bytes: seq[byte]): string =
  result = newString(bytes.len)
  for index, value in bytes:
    result[index] = char(value)

proc readReply(connection: RedisConnection): Future[RespValue] {.async.} =
  while true:
    var buffer = newSeq[byte](4096)
    let count = await connection.transport.readOnce(
      addr buffer[0],
      buffer.len
    )
    if count == 0:
      raise newException(BackendConnectionError, "Redis connection closed")
    buffer.setLen(count)
    let values = connection.parser.feed(bytesToString(buffer))
    if values.len > 0:
      if values.len > 1:
        raise newException(
          ProtocolError,
          "surplus reply on non-pipelined Redis connection"
        )
      return values[0]

proc sendRawImpl(connection: RedisConnection,
    arguments: seq[string]): Future[RespValue] {.async.} =
  let request = encodeCommand(arguments)
  discard await connection.transport.write(request)
  return await connection.readReply()

proc sendRaw(connection: RedisConnection,
    arguments: openArray[string]): Future[RespValue] =
  connection.sendRawImpl(@arguments)

proc checkHandshakeReply(reply: RespValue, expected: string,
    authentication = false) =
  if reply.kind == rkError:
    if authentication:
      raise newException(
        RedisAuthenticationError,
        "Redis authentication failed"
      )
    raise newException(RedisCommandError, "Redis handshake command failed")
  if reply.kind != rkSimpleString or reply.text != expected:
    raise newException(ProtocolError, "unexpected Redis handshake reply")

proc remaining(deadline: Moment): Duration =
  let now = Moment.now()
  if deadline <= now: 0.nanoseconds else: deadline - now

proc establish(connection: RedisConnection, deadline: Moment) {.async.} =
  if connection.transport != nil:
    return
  var addresses: seq[TransportAddress]
  try:
    addresses = resolveTAddress(
      connection.config.host,
      Port(connection.config.port)
    )
  except CancelledError:
    raise
  except CatchableError:
    raise newException(BackendConnectionError, "Redis host resolution failed")
  if addresses.len == 0:
    raise newException(BackendConnectionError, "Redis host resolution failed")
  let connectFuture = connect(addresses[0])
  try:
    let budget = min(connection.options.connectTimeout, deadline.remaining())
    if budget <= 0.nanoseconds:
      raise newException(BackendTimeoutError, "Redis connect timed out")
    connection.transport =
      await connectFuture.wait(budget)
  except AsyncTimeoutError:
    await connectFuture.cancelAndWait()
    raise newException(BackendTimeoutError, "Redis connect timed out")
  except CancelledError:
    await connectFuture.cancelAndWait()
    raise
  except TransportError:
    raise newException(BackendConnectionError, "Redis connect failed")

  try:
    if connection.config.password.isSome:
      let reply =
        if connection.config.username.isSome:
          await connection.sendRaw([
            "AUTH",
            connection.config.username.get,
            connection.config.password.get
          ])
        else:
          await connection.sendRaw(["AUTH", connection.config.password.get])
      checkHandshakeReply(reply, "OK", authentication = true)
    if connection.config.database != 0:
      checkHandshakeReply(
        await connection.sendRaw(["SELECT", $connection.config.database]),
        "OK"
      )
    checkHandshakeReply(await connection.sendRaw(["PING"]), "PONG")
  except CatchableError:
    await connection.disconnect()
    raise

proc executeImpl(connection: RedisConnection,
    arguments: seq[string]): Future[RespValue] {.async.} =
  if connection.isClosed:
    raise newException(BackendClosedError, "Redis connection is closed")
  if arguments.len == 0:
    raise newException(InvalidArgumentError, "Redis command must not be empty")

  var acquired = false
  let deadline = Moment.now() + connection.options.operationTimeout
  try:
    let acquireFuture = connection.lock.acquire()
    try:
      let budget = deadline.remaining()
      if budget <= 0.nanoseconds:
        raise newException(BackendTimeoutError, "Redis operation timed out")
      await acquireFuture.wait(budget)
      acquired = true
    except AsyncTimeoutError:
      await acquireFuture.cancelAndWait()
      raise newException(BackendTimeoutError, "Redis operation timed out")

    if connection.isClosed:
      raise newException(BackendClosedError, "Redis connection is closed")

    let establishment = connection.establish(deadline)
    try:
      let budget = deadline.remaining()
      if budget <= 0.nanoseconds:
        raise newException(BackendTimeoutError, "Redis handshake timed out")
      await establishment.wait(budget)
    except AsyncTimeoutError:
      await establishment.cancelAndWait()
      await connection.disconnect()
      raise newException(BackendTimeoutError, "Redis handshake timed out")
    let operation = connection.sendRaw(arguments)
    try:
      let budget = deadline.remaining()
      if budget <= 0.nanoseconds:
        raise newException(BackendTimeoutError, "Redis operation timed out")
      result = await operation.wait(budget)
    except AsyncTimeoutError:
      await operation.cancelAndWait()
      await connection.disconnect()
      raise newException(BackendTimeoutError, "Redis operation timed out")
    if result.kind == rkError:
      raise newException(RedisCommandError, "Redis command failed")
  except CancelledError:
    if acquired:
      await connection.disconnect()
    raise
  except TransportError:
    await connection.disconnect()
    raise newException(BackendConnectionError, "Redis connection failed")
  except BackendConnectionError:
    await connection.disconnect()
    raise
  except ProtocolError:
    await connection.disconnect()
    raise
  finally:
    if acquired:
      connection.lock.release()

proc execute*(connection: RedisConnection,
    arguments: openArray[string]): Future[RespValue] =
  connection.executeImpl(@arguments)

proc close*(connection: RedisConnection): Future[void] {.async.} =
  if connection.isClosed:
    return
  connection.isClosed = true
  await connection.lock.acquire()
  try:
    await connection.disconnect()
  finally:
    connection.lock.release()
