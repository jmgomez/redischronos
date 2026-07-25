import std/[options, parseutils, uri]

import ./errors

type RedisConfig* = object
  host*: string
  port*: uint16
  database*: int
  username*: Option[string]
  password*: Option[string]

proc invalidUrl(message: string): ref InvalidBackendUrlError =
  newException(InvalidBackendUrlError, "invalid Redis URL: " & message)

proc parseDecimal(text, field: string): int =
  var value: BiggestInt
  let consumed = parseBiggestInt(text, value)
  if text.len == 0 or consumed != text.len or value < 0 or value > high(int):
    raise invalidUrl(field & " is invalid")
  int(value)

proc decodeCredential(text: string): string =
  try:
    decodeUrl(text, decodePlus = false)
  except CatchableError:
    raise invalidUrl("credentials contain invalid percent encoding")

proc parseRedisUrl*(value: string): RedisConfig =
  var parsed: Uri
  try:
    parsed = parseUri(value)
  except CatchableError:
    raise invalidUrl("syntax is invalid")

  if parsed.scheme == "rediss":
    raise invalidUrl("rediss:// is unsupported")
  if parsed.scheme != "redis":
    raise invalidUrl("scheme must be redis://")
  if parsed.hostname.len == 0:
    raise invalidUrl("host is required")
  if parsed.query.len > 0 or parsed.anchor.len > 0:
    raise invalidUrl("query and fragment components are unsupported")

  result.host = parsed.hostname
  result.port = 6379
  result.database = 0

  if parsed.port.len > 0:
    let port = parseDecimal(parsed.port, "port")
    if port == 0 or port > int(high(uint16)):
      raise invalidUrl("port is invalid")
    result.port = uint16(port)

  if parsed.path.len > 0 and parsed.path != "/":
    if parsed.path[0] != '/' or '/' in parsed.path[1 .. ^1]:
      raise invalidUrl("database path is invalid")
    result.database = parseDecimal(parsed.path[1 .. ^1], "database")

  if '@' in value or parsed.username.len > 0 or parsed.password.len > 0:
    if parsed.password.len == 0:
      raise invalidUrl("password must not be empty")
    if parsed.username.len > 0:
      result.username = some(decodeCredential(parsed.username))
    result.password = some(decodeCredential(parsed.password))

func `$`*(config: RedisConfig): string =
  let host =
    if ':' in config.host: "[" & config.host & "]"
    else: config.host
  "redis://" & host & ":" & $config.port & "/" & $config.database
