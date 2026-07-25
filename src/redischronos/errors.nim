type
  RedisChronosError* = object of CatchableError
  InvalidArgumentError* = object of RedisChronosError
  InvalidBackendUrlError* = object of RedisChronosError
  BackendClosedError* = object of RedisChronosError
  BackendTimeoutError* = object of RedisChronosError
  BackendConnectionError* = object of RedisChronosError
  RedisAuthenticationError* = object of RedisChronosError
  RedisCommandError* = object of RedisChronosError
  ProtocolError* = object of RedisChronosError
