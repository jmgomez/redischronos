import std/[strutils, unittest]
import redischronos/errors
import redischronos/resp2

suite "RESP2 encoder":
  test "encodes every RESP2 value shape byte-for-byte":
    let cases = @[
      (simpleString("OK"), "+OK\r\n"),
      (errorValue("ERR failure"), "-ERR failure\r\n"),
      (integerValue(-42), ":-42\r\n"),
      (bulkString(""), "$0\r\n\r\n"),
      (bulkString("a\0b"), "$3\r\na\0b\r\n"),
      (nilBulkString(), "$-1\r\n"),
      (arrayValue(@[simpleString("OK"), integerValue(2)]),
        "*2\r\n+OK\r\n:2\r\n"),
      (arrayValue(@[arrayValue(@[bulkString("nested")])]),
        "*1\r\n*1\r\n$6\r\nnested\r\n"),
      (nilArray(), "*-1\r\n")
    ]
    for testCase in cases:
      check encode(testCase[0]) == testCase[1]

  test "command arguments are always bulk-framed":
    check encodeCommand(["SET", "key\r\n$5\r\nowned", "a\0b"]) ==
      "*3\r\n$3\r\nSET\r\n$14\r\nkey\r\n$5\r\nowned\r\n$3\r\na\0b\r\n"

  test "simple strings and errors cannot inject frames":
    expect ProtocolError:
      discard encode(simpleString("OK\r\n:1"))
    expect ProtocolError:
      discard encode(errorValue("ERR\r\n+OK"))

suite "RESP2 incremental parser":
  let fixtures = @[
    (simpleString("OK"), "+OK\r\n"),
    (errorValue("ERR failure"), "-ERR failure\r\n"),
    (integerValue(-42), ":-42\r\n"),
    (bulkString(""), "$0\r\n\r\n"),
    (bulkString("a\0b"), "$3\r\na\0b\r\n"),
    (nilBulkString(), "$-1\r\n"),
    (arrayValue(@[
      simpleString("OK"),
      arrayValue(@[bulkString("nested"), nilBulkString()])
    ]), "*2\r\n+OK\r\n*2\r\n$6\r\nnested\r\n$-1\r\n"),
    (nilArray(), "*-1\r\n")
  ]

  test "parses fixtures whole and byte-by-byte":
    for fixture in fixtures:
      var parser = newRespParser()
      check parser.feed(fixture[1]) == @[fixture[0]]
      parser.finish()

      parser = newRespParser()
      var values: seq[RespValue]
      for character in fixture[1]:
        values.add(parser.feed($character))
      check values == @[fixture[0]]
      parser.finish()

  test "parses fixtures split at every boundary":
    for fixture in fixtures:
      for boundary in 0 .. fixture[1].len:
        let parser = newRespParser()
        var values = parser.feed(fixture[1][0 ..< boundary])
        values.add(parser.feed(fixture[1][boundary ..< fixture[1].len]))
        check values == @[fixture[0]]
        parser.finish()

  test "returns every concatenated frame":
    let parser = newRespParser()
    check parser.feed("+OK\r\n:1\r\n$-1\r\n") ==
      @[simpleString("OK"), integerValue(1), nilBulkString()]

  test "rejects malformed and oversized frames":
    for invalid in [
      "?unknown\r\n",
      ":abc\r\n",
      "$-2\r\n",
      "*-2\r\n",
      "$3\r\nabcXX",
      "$999\r\n",
      "*999\r\n"
    ]:
      let parser = newRespParser(maxBulkBytes = 8, maxArrayLength = 8)
      expect ProtocolError:
        discard parser.feed(invalid)

  test "enforces nesting and reports truncation":
    let shallowParser = newRespParser(maxDepth = 1)
    expect ProtocolError:
      discard shallowParser.feed("*1\r\n*1\r\n*1\r\n:1\r\n")

    for truncated in ["+", "$3\r\nab", "*2\r\n:1\r\n"]:
      let parser = newRespParser()
      discard parser.feed(truncated)
      expect ProtocolError:
        parser.finish()

  test "bounds total buffered input":
    let parser = newRespParser(maxBufferBytes = 8)
    discard parser.feed("$8\r\n")
    expect ProtocolError:
      discard parser.feed("123456789")

  test "inline limits are invariant to fragmentation":
    let validFrames = [
      "+" & repeat("x", 128) & "\r\n",
      "-ERR " & repeat("failure ", 20) & "\r\n",
      ":9223372036854775807\r\n",
      "$128\r\n" & repeat("x", 128) & "\r\n"
    ]
    for frame in validFrames:
      let whole = newRespParser(maxInlineBytes = 256).feed(frame)
      for boundary in 0 .. frame.len:
        let parser = newRespParser(maxInlineBytes = 256)
        var fragmented = parser.feed(frame[0 ..< boundary])
        fragmented.add(parser.feed(frame[boundary ..< frame.len]))
        check fragmented == whole

      let byteParser = newRespParser(maxInlineBytes = 256)
      var byteValues: seq[RespValue]
      for character in frame:
        byteValues.add(byteParser.feed($character))
      check byteValues == whole

    for fragments in [
      @["+" & repeat("x", 17), "\r\n"],
      @["+" & repeat("x", 8), repeat("x", 9) & "\r\n"]
    ]:
      let parser = newRespParser(maxInlineBytes = 16)
      expect ProtocolError:
        for fragment in fragments:
          discard parser.feed(fragment)
