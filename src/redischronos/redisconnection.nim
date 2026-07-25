import std/options
import chronos

import ./errors
import ./options as backendoptions
import ./redisurl
import ./redisresolve
import ./resp2

type RedisConnection* = ref object
  config: RedisConfig
  options: BackendOptions
  lock: AsyncLock
  transport: StreamTransport
  parser: RespParser
  isClosed: bool
  closeTask: Future[void]

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
      if values.len > 1 or connection.parser.bufferedBytes > 0:
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
  let addresses = await resolveRedisAddresses(
    connection.config.host,
    connection.config.port,
    deadline.remaining()
  )
  let connectDeadline = Moment.now() + connection.options.connectTimeout
  for address in addresses:
    let budget = min(connectDeadline.remaining(), deadline.remaining())
    if budget <= 0.nanoseconds:
      raise newException(BackendTimeoutError, "Redis connect timed out")
    let connectFuture = connect(address)
    try:
      connection.transport = await connectFuture.wait(budget)
      break
    except AsyncTimeoutError:
      await connectFuture.cancelAndWait()
    except CancelledError:
      await connectFuture.cancelAndWait()
      raise
    except TransportError:
      discard
  if connection.transport == nil:
    if connectDeadline.remaining() <= 0.nanoseconds:
      raise newException(BackendTimeoutError, "Redis connect timed out")
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
  let started = Moment.now()
  let admissionDeadline = started + connection.options.operationTimeout
  try:
    let admissionBudget = admissionDeadline.remaining()
    if admissionBudget <= 0.nanoseconds:
      raise newException(BackendTimeoutError, "Redis operation timed out")
    let acquireFuture = connection.lock.acquire()
    try:
      await acquireFuture.wait(admissionBudget)
      acquired = true
    except AsyncTimeoutError:
      await acquireFuture.cancelAndWait()
      raise newException(BackendTimeoutError, "Redis operation timed out")

    if connection.isClosed:
      raise newException(BackendClosedError, "Redis connection is closed")

    let establishmentDeadline =
      Moment.now() + connection.options.connectTimeout
    let establishment = connection.establish(establishmentDeadline)
    try:
      let budget = establishmentDeadline.remaining()
      if budget <= 0.nanoseconds:
        raise newException(BackendTimeoutError, "Redis handshake timed out")
      await establishment.wait(budget)
    except AsyncTimeoutError:
      await establishment.cancelAndWait()
      await connection.disconnect()
      raise newException(BackendTimeoutError, "Redis handshake timed out")
    let operationDeadline =
      Moment.now() + connection.options.operationTimeout
    let operation = connection.sendRaw(arguments)
    try:
      let budget = operationDeadline.remaining()
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

proc closeOwned(connection: RedisConnection) {.async.} =
  await connection.lock.acquire()
  try:
    await connection.disconnect()
  finally:
    connection.lock.release()

proc joinClose(connection: RedisConnection): Future[void] {.async.} =
  if connection.closeTask == nil:
    connection.isClosed = true
    connection.closeTask = connection.closeOwned()
  await connection.closeTask.noCancel()

proc close*(connection: RedisConnection): Future[void] =
  connection.joinClose()

when defined(test):
  proc setOperationTimeoutForTest*(connection: RedisConnection,
      timeout: Duration) =
    connection.options.operationTimeout = timeout
