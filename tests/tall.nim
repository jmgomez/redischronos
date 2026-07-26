import std/unittest
import chronos
import redischronos

import ./tmemorykv
import ./tmemorypubsub
import ./tcontractharness
import ./tcache
import ./tresp2
import ./tredisurl
import ./tredisresolve
import ./tredisconnection
import ./trediskv
import ./tredispubsub
import ./tredispubsubfaults
import ./tredisoutage

suite "public contracts":
  test "exposes the stable release version":
    check redischronosVersion == "1.1.2"

  test "default options match the documented contract":
    let options = defaultBackendOptions()
    check options.connectTimeout == 5.seconds
    check options.operationTimeout == 2.seconds
    check options.memoryMaxEntries == 4096
    check options.pubSubMaxPendingMessages == 1024
    check options.reconnectJitterSource == nil
    check options.onHandlerError == nil

  test "all public failures share one typed base":
    check InvalidArgumentError is RedisChronosError
    check InvalidBackendUrlError is RedisChronosError
    check BackendClosedError is RedisChronosError
    check BackendTimeoutError is RedisChronosError
    check BackendConnectionError is RedisChronosError
    check RedisAuthenticationError is RedisChronosError
    check RedisCommandError is RedisChronosError
    check ProtocolError is RedisChronosError

  test "empty and memory URLs open backend-neutral handles":
    let emptyStore = waitFor openKvStore("")
    let memoryStore = waitFor openKvStore("mem://")
    let emptyBus = waitFor openPubSub("")
    let memoryBus = waitFor openPubSub("mem://")

    check emptyStore != nil
    check memoryStore != nil
    check emptyBus != nil
    check memoryBus != nil

    waitFor emptyStore.close()
    waitFor emptyStore.close()
    waitFor memoryStore.close()
    waitFor emptyBus.close()
    waitFor emptyBus.close()
    waitFor memoryBus.close()

  test "unknown schemes fail rather than falling back":
    expect InvalidBackendUrlError:
      discard waitFor openKvStore("other://example")
    expect InvalidBackendUrlError:
      discard waitFor openPubSub("other://example")

  test "subscription identity and handler observer are public":
    var observed = false
    proc observer(error: HandlerError) {.gcsafe, raises: [].} =
      observed = error.channel == "events" and error.subscriptionId == 7'u64

    let options = BackendOptions(onHandlerError: observer)
    let error = HandlerError(
      channel: "events",
      subscriptionId: 7,
      cause: newException(ValueError, "handler failed")
    )
    options.onHandlerError(error)
    check observed

    var first, second: Subscription
    new(first)
    new(second)
    check first != second

  test "the complete abstract surface type-checks":
    proc useApi(store: KvStore, bus: PubSub, subscription: Subscription) {.
        used.} =
      var touched = false
      let messageHandler: MessageHandler =
        proc(channel, payload: string): Future[void] {.async.} =
          touched = channel.len + payload.len > 0
      let stateHandler: StateHandler =
        proc(state: ConnectionState): Future[void] {.async.} =
          touched = state == csConnected

      discard store.get("key")
      discard store.set("key", "value")
      discard store.delete("key")
      discard store.exists("key")
      discard store.increment("key")
      discard store.close()
      discard bus.subscribe("channel", messageHandler)
      discard bus.unsubscribe(subscription)
      discard bus.publish("channel", "payload")
      bus.onStateChange(stateHandler)
      discard bus.close()
      discard touched

    check compiles(useApi(nil, nil, nil))
