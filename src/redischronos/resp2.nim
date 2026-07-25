import std/parseutils

import ./errors

type
  RespKind* = enum
    rkSimpleString,
    rkError,
    rkInteger,
    rkBulkString,
    rkNilBulkString,
    rkArray,
    rkNilArray

  RespValue* = ref object
    case kind*: RespKind
    of rkSimpleString, rkError, rkBulkString:
      text*: string
    of rkInteger:
      integer*: int64
    of rkArray:
      items*: seq[RespValue]
    of rkNilBulkString, rkNilArray:
      discard

  RespParser* = ref object
    buffer: string
    maxBulkBytes: int
    maxArrayLength: int
    maxDepth: int
    maxBufferBytes: int
    maxInlineBytes: int

  ParseStatus = enum
    psIncomplete, psComplete

func simpleString*(value: string): RespValue =
  RespValue(kind: rkSimpleString, text: value)

func errorValue*(value: string): RespValue =
  RespValue(kind: rkError, text: value)

func integerValue*(value: int64): RespValue =
  RespValue(kind: rkInteger, integer: value)

func bulkString*(value: string): RespValue =
  RespValue(kind: rkBulkString, text: value)

func nilBulkString*(): RespValue =
  RespValue(kind: rkNilBulkString)

func arrayValue*(values: openArray[RespValue]): RespValue =
  RespValue(kind: rkArray, items: @values)

func nilArray*(): RespValue =
  RespValue(kind: rkNilArray)

func `==`*(left, right: RespValue): bool =
  if left.isNil or right.isNil:
    return left.isNil and right.isNil
  if left.kind != right.kind:
    return false
  case left.kind
  of rkSimpleString, rkError, rkBulkString:
    left.text == right.text
  of rkInteger:
    left.integer == right.integer
  of rkArray:
    left.items == right.items
  of rkNilBulkString, rkNilArray:
    true

proc requireInlineValue(value: string) =
  if '\r' in value or '\n' in value:
    raise newException(
      ProtocolError,
      "RESP inline value contains a line break"
    )

proc encode*(value: RespValue): string =
  if value == nil:
    raise newException(ProtocolError, "cannot encode a nil RESP value")
  case value.kind
  of rkSimpleString:
    requireInlineValue(value.text)
    result = "+" & value.text & "\r\n"
  of rkError:
    requireInlineValue(value.text)
    result = "-" & value.text & "\r\n"
  of rkInteger:
    result = ":" & $value.integer & "\r\n"
  of rkBulkString:
    result = "$" & $value.text.len & "\r\n" & value.text & "\r\n"
  of rkNilBulkString:
    result = "$-1\r\n"
  of rkArray:
    result = "*" & $value.items.len & "\r\n"
    for item in value.items:
      result.add(encode(item))
  of rkNilArray:
    result = "*-1\r\n"

proc encodeCommand*(arguments: openArray[string]): string =
  result = "*" & $arguments.len & "\r\n"
  for argument in arguments:
    result.add("$" & $argument.len & "\r\n")
    result.add(argument)
    result.add("\r\n")

proc newRespParser*(maxBulkBytes = 16 * 1024 * 1024,
    maxArrayLength = 1024, maxDepth = 32,
    maxBufferBytes = 32 * 1024 * 1024,
    maxInlineBytes = 64 * 1024): RespParser =
  if maxBulkBytes < 0 or maxArrayLength < 0 or maxDepth < 0 or
      maxBufferBytes < 0 or maxInlineBytes < 0:
    raise newException(ProtocolError, "RESP parser limits must not be negative")
  RespParser(
    maxBulkBytes: maxBulkBytes,
    maxArrayLength: maxArrayLength,
    maxDepth: maxDepth,
    maxBufferBytes: maxBufferBytes,
    maxInlineBytes: maxInlineBytes
  )

proc parseLine(parser: RespParser, position: int,
    line: var string, nextPosition: var int): ParseStatus =
  var index = position
  while index + 1 < parser.buffer.len:
    if parser.buffer[index] == '\r' and parser.buffer[index + 1] == '\n':
      if index - position > parser.maxInlineBytes:
        raise newException(ProtocolError, "RESP line exceeds parser limit")
      line = parser.buffer[position ..< index]
      nextPosition = index + 2
      return psComplete
    inc index
  if parser.buffer.len - position > parser.maxInlineBytes:
    raise newException(ProtocolError, "RESP line exceeds parser limit")
  psIncomplete

proc parseNumber(text: string, context: string): int64 =
  var value: BiggestInt
  let consumed = parseBiggestInt(text, value)
  if consumed != text.len or text.len == 0:
    raise newException(ProtocolError, "invalid RESP " & context)
  int64(value)

proc parseValue(parser: RespParser, position, depth: int,
    value: var RespValue, nextPosition: var int): ParseStatus =
  if position >= parser.buffer.len:
    return psIncomplete
  if depth > parser.maxDepth:
    raise newException(ProtocolError, "RESP nesting limit exceeded")

  let prefix = parser.buffer[position]
  var line: string
  var contentPosition: int
  case prefix
  of '+', '-', ':', '$', '*':
    if parser.parseLine(position + 1, line, contentPosition) == psIncomplete:
      return psIncomplete
  else:
    raise newException(ProtocolError, "unknown RESP type prefix")

  case prefix
  of '+':
    value = simpleString(line)
    nextPosition = contentPosition
  of '-':
    value = errorValue(line)
    nextPosition = contentPosition
  of ':':
    value = integerValue(parseNumber(line, "integer"))
    nextPosition = contentPosition
  of '$':
    let length = parseNumber(line, "bulk length")
    if length == -1:
      value = nilBulkString()
      nextPosition = contentPosition
    elif length < 0:
      raise newException(ProtocolError, "invalid RESP bulk length")
    elif length > int64(parser.maxBulkBytes):
      raise newException(ProtocolError, "RESP bulk length exceeds limit")
    else:
      let byteLength = int(length)
      if contentPosition > high(int) - byteLength - 2:
        raise newException(ProtocolError, "RESP bulk length overflows")
      let ending = contentPosition + byteLength
      if parser.buffer.len < ending + 2:
        return psIncomplete
      if parser.buffer[ending] != '\r' or parser.buffer[ending + 1] != '\n':
        raise newException(ProtocolError, "RESP bulk string lacks CRLF")
      value = bulkString(parser.buffer[contentPosition ..< ending])
      nextPosition = ending + 2
  of '*':
    let length = parseNumber(line, "array length")
    if length == -1:
      value = nilArray()
      nextPosition = contentPosition
    elif length < 0:
      raise newException(ProtocolError, "invalid RESP array length")
    elif length > int64(parser.maxArrayLength):
      raise newException(ProtocolError, "RESP array length exceeds limit")
    else:
      var items = newSeqOfCap[RespValue](int(length))
      var itemPosition = contentPosition
      for _ in 0 ..< int(length):
        var item: RespValue
        var following: int
        if parser.parseValue(itemPosition, depth + 1, item,
            following) == psIncomplete:
          return psIncomplete
        items.add(item)
        itemPosition = following
      value = arrayValue(items)
      nextPosition = itemPosition
  else:
    discard
  psComplete

proc feed*(parser: RespParser, data: string): seq[RespValue] =
  if data.len > parser.maxBufferBytes - parser.buffer.len:
    raise newException(ProtocolError, "RESP buffered data exceeds limit")
  parser.buffer.add(data)
  var position = 0
  while position < parser.buffer.len:
    var value: RespValue
    var nextPosition: int
    if parser.parseValue(position, 0, value, nextPosition) == psIncomplete:
      break
    result.add(value)
    position = nextPosition
  if position > 0:
    parser.buffer = parser.buffer[position ..< parser.buffer.len]

proc finish*(parser: RespParser) =
  if parser.buffer.len > 0:
    raise newException(ProtocolError, "truncated RESP frame")
