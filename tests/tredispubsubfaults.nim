import std/[unittest]
import chronos
import redischronos
import redischronos/redispubsub
import redischronos/resp2

proc bytesToString(bytes: seq[byte]): string =
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
    let values = parser.feed(bytesToString(bytes))
    if values.len > 0:
      return values[0]

proc commandName(value: RespValue): string =
  if value.kind == rkArray and value.items.len > 0 and
      value.items[0].kind == rkBulkString:
    value.items[0].text
  else:
    ""

proc commandArgument(value: RespValue, index: int): string =
  if value.kind == rkArray and index < value.items.len and
      value.items[index].kind == rkBulkString:
    value.items[index].text
  else:
    ""

type FaultServer = object
  server: StreamServer
  clients: AsyncQueue[StreamTransport]

proc startFaultServer(): FaultServer =
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

proc redisUrl(server: FaultServer): string =
  "redis://127.0.0.1:" & $server.server.localAddress().port

proc closeServer(server: FaultServer) {.async.} =
  server.server.stop()
  await server.server.closeWait()

proc subscribeAck(channel: string): string =
  encode(arrayValue([
    bulkString("subscribe"),
    bulkString(channel),
    integerValue(1)
  ]))

suite "Redis Pub/Sub fault injection":
  test "subscribe timeout rolls back and retry owns one live handle":
    proc exercise() {.async.} =
      let server = startFaultServer()
      var options = defaultBackendOptions()
      options.operationTimeout = 30.milliseconds
      options.reconnectJitterSource =
        proc(maxInclusive: int): int {.gcsafe, raises: [].} = 0
      let channel = "transactional"
      var firstEof = false
      let serverTask = proc() {.async.} =
        block timedOut:
          let client = await server.clients.popFirst()
          let parser = newRespParser()
          discard await readFrame(client, parser)
          discard await client.write("+PONG\r\n")
          check commandName(await readFrame(client, parser)) == "SUBSCRIBE"
          await sleepAsync(60.milliseconds)
          var byte: byte
          firstEof = (await client.readOnce(addr byte, 1)) == 0
          await client.closeWait()

        let client = await server.clients.popFirst()
        let parser = newRespParser()
        discard await readFrame(client, parser)
        discard await client.write("+PONG\r\n")
        check commandName(await readFrame(client, parser)) == "SUBSCRIBE"
        discard await client.write(subscribeAck(channel))
        var byte: byte
        discard await client.readOnce(addr byte, 1)
        await client.closeWait()

      let serving = serverTask()
      let bus = await openPubSub(server.redisUrl(), options)
      var states: seq[ConnectionState]
      bus.onStateChange(
        proc(state: ConnectionState): Future[void] {.async.} =
          states.add(state)
      )
      let handler: MessageHandler =
        proc(channel, payload: string): Future[void] {.async.} =
          discard
      expect BackendTimeoutError:
        discard await bus.subscribe(channel, handler)
      for _ in 0 ..< 200:
        if firstEof and csDisconnected in states and
            states[^1] == csConnected:
          break
        await sleepAsync(10.milliseconds)
      check firstEof
      check states[^1] == csConnected
      let retry = await bus.subscribe(channel, handler)
      check retry != nil
      await bus.close()
      await serving
      await server.closeServer()
    waitFor exercise()

  test "reconnect reconciliation performs linear membership work":
    proc exercise() {.async.} =
      const channelCount = 2000
      let server = startFaultServer()
      var options = defaultBackendOptions()
      options.operationTimeout = 10.seconds
      options.reconnectJitterSource =
        proc(maxInclusive: int): int {.gcsafe, raises: [].} = 0
      let serverTask = proc() {.async.} =
        let initial = await server.clients.popFirst()
        let initialParser = newRespParser()
        check commandName(await readFrame(initial, initialParser)) == "PING"
        discard await initial.write("+PONG\r\n")
        var byte: byte
        discard await initial.readOnce(addr byte, 1)
        await initial.closeWait()

        let candidate = await server.clients.popFirst()
        let parser = newRespParser()
        check commandName(await readFrame(candidate, parser)) == "PING"
        discard await candidate.write("+PONG\r\n")
        for _ in 0 ..< channelCount:
          let command = await readFrame(candidate, parser)
          check commandName(command) == "SUBSCRIBE"
          discard await candidate.write(
            subscribeAck(commandArgument(command, 1))
          )
        discard await candidate.readOnce(addr byte, 1)
        await candidate.closeWait()

      let serving = serverTask()
      let bus = await openPubSub(server.redisUrl(), options)
      bus.seedDesiredChannelsForTest(channelCount)
      let recovered = newFuture[void]("linear reconciliation recovered")
      var disconnected = false
      bus.onStateChange(
        proc(state: ConnectionState): Future[void] {.async.} =
          if state == csDisconnected:
            disconnected = true
          elif disconnected and state == csConnected and
              not recovered.finished:
            recovered.complete()
      )
      await bus.disconnectSubscriberForTest()
      await recovered.wait(12.seconds)
      check bus.reconciliationChecksForTest() <= channelCount * 5
      await bus.close()
      await serving
      await server.closeServer()
    waitFor exercise()

  test "failed reconnect candidates close and a later attempt converges":
    proc exercise() {.async.} =
      let server = startFaultServer()
      var options = defaultBackendOptions()
      options.operationTimeout = 30.milliseconds
      options.connectTimeout = 100.milliseconds
      options.reconnectJitterSource =
        proc(maxInclusive: int): int {.gcsafe, raises: [].} = 0
      let channel = "faults"
      let replacementChannel = "faults-replacement"
      var rejectedEofs = 0
      let stalledAck = newFuture[void]("stalled reconnect acknowledgement")

      let serverTask = proc() {.async.} =
        block initial:
          let client = await server.clients.popFirst()
          let parser = newRespParser()
          check commandName(await readFrame(client, parser)) == "PING"
          discard await client.write("+PONG\r\n")
          check commandName(await readFrame(client, parser)) == "SUBSCRIBE"
          discard await client.write(subscribeAck(channel))
          await sleepAsync(20.milliseconds)
          client.close()
          await client.closeWait()

        for attempt in 0 ..< 3:
          let client = await server.clients.popFirst()
          let parser = newRespParser()
          check commandName(await readFrame(client, parser)) == "PING"
          discard await client.write("+PONG\r\n")
          let subscribe = await readFrame(client, parser)
          check commandName(subscribe) == "SUBSCRIBE"
          let expectedChannel =
            if attempt < 2: channel else: replacementChannel
          check commandArgument(subscribe, 1) == expectedChannel
          if attempt == 1:
            stalledAck.complete()
            await sleepAsync(60.milliseconds)
          else:
            discard await client.write("?malformed\r\n")
          var byte: byte
          let count = await client.readOnce(addr byte, 1)
          if count == 0:
            inc rejectedEofs
          await client.closeWait()

        let client = await server.clients.popFirst()
        let parser = newRespParser()
        check commandName(await readFrame(client, parser)) == "PING"
        discard await client.write("+PONG\r\n")
        let subscribe = await readFrame(client, parser)
        check commandName(subscribe) == "SUBSCRIBE"
        check commandArgument(subscribe, 1) == replacementChannel
        discard await client.write(subscribeAck(replacementChannel))
        var byte: byte
        discard await client.readOnce(addr byte, 1)
        await client.closeWait()

      let serving = serverTask()
      let bus = await openPubSub(server.redisUrl(), options)
      var states: seq[ConnectionState]
      bus.onStateChange(
        proc(state: ConnectionState): Future[void] {.async.} =
          states.add(state)
      )
      let handler: MessageHandler =
        proc(channel, payload: string): Future[void] {.async.} =
          discard
      let original = await bus.subscribe(channel, handler)
      await stalledAck
      discard await bus.subscribe(replacementChannel, handler)
      await bus.unsubscribe(original)
      for _ in 0 ..< 300:
        if rejectedEofs == 3 and states.len > 0 and
            states[^1] == csConnected:
          break
        await sleepAsync(10.milliseconds)
      check rejectedEofs == 3
      check states[^1] == csConnected
      await bus.close()
      await serving
      await server.closeServer()
    waitFor exercise()
