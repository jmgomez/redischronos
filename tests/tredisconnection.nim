import std/[options, unittest]
import chronos
import redischronos
import redischronos/redisconnection
import redischronos/redisurl
import redischronos/resp2

proc toString(bytes: seq[byte]): string =
  result = newString(bytes.len)
  for index, value in bytes:
    result[index] = char(value)

proc readFrame(client: StreamTransport,
    parser: RespParser): Future[RespValue] {.async.} =
  while true:
    var bytes = newSeq[byte](4096)
    let count = await client.readOnce(addr bytes[0], bytes.len)
    if count == 0:
      raise newException(ValueError, "client disconnected")
    bytes.setLen(count)
    let values = parser.feed(toString(bytes))
    if values.len > 0:
      return values[0]

proc commandName(value: RespValue): string =
  if value.kind == rkArray and value.items.len > 0 and
      value.items[0].kind == rkBulkString:
    value.items[0].text
  else:
    ""

type TestServer = object
  server: StreamServer
  clients: AsyncQueue[StreamTransport]

proc startServer(): TestServer =
  result.clients = newAsyncQueue[StreamTransport]()
  let clients = result.clients
  proc accepted(server: StreamServer,
      client: StreamTransport) {.async: (raises: []).} =
    try:
      await clients.put(client)
    except CancelledError:
      client.close()
  result.server = createStreamServer(
    initTAddress("127.0.0.1", 0),
    accepted
  )
  result.server.start()

proc configFor(server: TestServer): RedisConfig =
  RedisConfig(
    host: "127.0.0.1",
    port: uint16(server.server.localAddress().port),
    database: 0
  )

proc closeServer(server: TestServer) {.async.} =
  server.server.stop()
  await server.server.closeWait()

suite "serialized Redis connection":
  test "performs AUTH SELECT PING and correlates serialized commands":
    proc exercise() {.async.} =
      let server = startServer()
      var config = configFor(server)
      config.username = some("user")
      config.password = some("password")
      config.database = 2
      let connection = newRedisConnection(config)

      let serverTask = proc() {.async.} =
        let client = await server.clients.popFirst()
        let parser = newRespParser()
        for expected in ["AUTH", "SELECT", "PING", "ONE", "TWO"]:
          let command = await readFrame(client, parser)
          check commandName(command) == expected
          let reply =
            case expected
            of "PING": "+PONG\r\n"
            of "ONE": ":1\r\n"
            of "TWO": ":2\r\n"
            else: "+OK\r\n"
          discard await client.write(reply)
        await client.closeWait()

      let serving = serverTask()
      let one = connection.execute(["ONE"])
      let two = connection.execute(["TWO"])
      check (await one) == integerValue(1)
      check (await two) == integerValue(2)
      await connection.close()
      await serving
      await server.closeServer()

    waitFor exercise()

  test "maps authentication and server command errors":
    proc exercise() {.async.} =
      let authServer = startServer()
      var authConfig = configFor(authServer)
      authConfig.password = some("secret")
      let authConnection = newRedisConnection(authConfig)
      let authTask = proc() {.async.} =
        let client = await authServer.clients.popFirst()
        discard await readFrame(client, newRespParser())
        discard await client.write("-ERR invalid credentials\r\n")
        await client.closeWait()
      let authServing = authTask()
      expect RedisAuthenticationError:
        discard await authConnection.execute(["GET", "key"])
      await authConnection.close()
      await authServing
      await authServer.closeServer()

      let commandServer = startServer()
      let commandConnection = newRedisConnection(configFor(commandServer))
      let commandTask = proc() {.async.} =
        let client = await commandServer.clients.popFirst()
        let parser = newRespParser()
        discard await readFrame(client, parser)
        discard await client.write("+PONG\r\n")
        discard await readFrame(client, parser)
        discard await client.write("-ERR failed\r\n")
        await client.closeWait()
      let commandServing = commandTask()
      expect RedisCommandError:
        discard await commandConnection.execute(["BAD"])
      await commandConnection.close()
      await commandServing
      await commandServer.closeServer()
    waitFor exercise()

  test "bounds stalled commands and rejects malformed replies":
    proc exercise() {.async.} =
      let timeoutServer = startServer()
      var options = defaultBackendOptions()
      options.operationTimeout = 20.milliseconds
      let timeoutConnection =
        newRedisConnection(configFor(timeoutServer), options)
      let timeoutTask = proc() {.async.} =
        let client = await timeoutServer.clients.popFirst()
        let parser = newRespParser()
        discard await readFrame(client, parser)
        discard await client.write("+PONG\r\n")
        discard await readFrame(client, parser)
        await sleepAsync(100.milliseconds)
        await client.closeWait()
      let timeoutServing = timeoutTask()
      expect BackendTimeoutError:
        discard await timeoutConnection.execute(["SLOW"])
      await timeoutConnection.close()
      await timeoutServing
      await timeoutServer.closeServer()

      let malformedServer = startServer()
      let malformedConnection =
        newRedisConnection(configFor(malformedServer))
      let malformedTask = proc() {.async.} =
        let client = await malformedServer.clients.popFirst()
        discard await readFrame(client, newRespParser())
        discard await client.write("?invalid\r\n")
        await client.closeWait()
      let malformedServing = malformedTask()
      expect ProtocolError:
        discard await malformedConnection.execute(["GET", "key"])
      await malformedConnection.close()
      await malformedServing
      await malformedServer.closeServer()
    waitFor exercise()

  test "close is idempotent and preserves cancellation":
    proc exercise() {.async.} =
      let server = startServer()
      let connection = newRedisConnection(configFor(server))
      await connection.close()
      await connection.close()
      expect BackendClosedError:
        discard await connection.execute(["PING"])
      await server.closeServer()
    waitFor exercise()

  test "disconnect fails the current command and the next command reconnects":
    proc exercise() {.async.} =
      let server = startServer()
      let connection = newRedisConnection(configFor(server))
      let serverTask = proc() {.async.} =
        var client = await server.clients.popFirst()
        var parser = newRespParser()
        discard await readFrame(client, parser)
        discard await client.write("+PONG\r\n")
        discard await readFrame(client, parser)
        await client.closeWait()

        client = await server.clients.popFirst()
        parser = newRespParser()
        discard await readFrame(client, parser)
        discard await client.write("+PONG\r\n")
        discard await readFrame(client, parser)
        discard await client.write(":7\r\n")
        await client.closeWait()

      let serving = serverTask()
      expect BackendConnectionError:
        discard await connection.execute(["FIRST"])
      check (await connection.execute(["SECOND"])) == integerValue(7)
      await connection.close()
      await serving
      await server.closeServer()
    waitFor exercise()

  test "cancellation remains cancellation and closes ambiguous I/O":
    proc exercise() {.async.} =
      let server = startServer()
      let connection = newRedisConnection(configFor(server))
      let serverTask = proc() {.async.} =
        let client = await server.clients.popFirst()
        let parser = newRespParser()
        discard await readFrame(client, parser)
        discard await client.write("+PONG\r\n")
        discard await readFrame(client, parser)
        await sleepAsync(100.milliseconds)
        await client.closeWait()
      let serving = serverTask()
      let operation = connection.execute(["BLOCK"])
      await sleepAsync(10.milliseconds)
      operation.cancelSoon()
      expect CancelledError:
        discard await operation
      await connection.close()
      await serving
      await server.closeServer()
    waitFor exercise()
