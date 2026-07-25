import std/[options, strutils, unittest]
import redischronos/errors
import redischronos/redisurl

suite "Redis URL parsing":
  test "defaults port and database":
    let config = parseRedisUrl("redis://localhost")
    check config.host == "localhost"
    check config.port == 6379
    check config.database == 0
    check config.username.isNone
    check config.password.isNone

  test "parses password and ACL authentication":
    let password = parseRedisUrl("redis://:p%40ss+word@example.com:6380/2")
    check password.password == some("p@ss+word")
    check password.username.isNone
    check password.port == 6380
    check password.database == 2

    let acl = parseRedisUrl("redis://user%20name:p%2Fass@localhost/15")
    check acl.username == some("user name")
    check acl.password == some("p/ass")

  test "parses IPv4 and bracketed IPv6":
    check parseRedisUrl("redis://127.0.0.1").host == "127.0.0.1"
    let ipv6 = parseRedisUrl("redis://[::1]:6381/3")
    check ipv6.host == "::1"
    check ipv6.port == 6381
    check $ipv6 == "redis://[::1]:6381/3"

  test "rejects invalid schemes, ports, databases, and credentials":
    for invalid in [
      "http://localhost",
      "rediss://localhost",
      "redis://",
      "redis://localhost:0",
      "redis://localhost:65536",
      "redis://localhost/not-a-number",
      "redis://localhost/-1",
      "redis://user@localhost",
      "redis://user:@localhost"
    ]:
      expect InvalidBackendUrlError:
        discard parseRedisUrl(invalid)

  test "formatted values and errors redact credentials":
    let secret = "highly-secret"
    let config = parseRedisUrl("redis://alice:" & secret & "@localhost/2")
    check secret notin $config
    check "alice" notin $config

    try:
      discard parseRedisUrl("redis://alice:" & secret & "@localhost/bad")
      check false
    except InvalidBackendUrlError as error:
      check secret notin error.msg
      check "alice" notin error.msg
