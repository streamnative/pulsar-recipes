# RPC

A distributed RPC framework. A limitation of traditional RPC frameworks is that they require a direct connection between
client and server. This system provides additional fault tolerance by using Apache Pulsar for the transport which allows
the direct interaction between client and server to be decoupled.

## Pulsar configuration

### Request topic discovery

When an RPC client creates a new request topic and sends the first request(s) there will be no RPC servers subscribed.
This creates a couple of challenges.

The first is that the time for the server to discover a request topic and process the first request(s) could exceed the
RPC client's timeout. The RPC server can alleviate this by configuring the `channelDiscoveryInterval` which configures
the underlying Pulsar client's `patternAutoDiscoveryPeriod`. To improve this further, Pulsar cluster operators can
enable the `enableBrokerSideSubscriptionPatternEvaluation` (present since: 2.11.0 and enabled by default) option in
`broker.conf` which not only improves the topic evaluation performance but means that topic discovery is very quick.

The second challenge is that because there are no consumers subscribed when the first request(s) are sent it means that
when the RPC server discovers the topic, those first requests will not have been retained for the server to
process them. A short message retention policy should be configured in this case.

## Example

### Shared

```java
@Value
class Request {
    String name;
}

@Value
class Response {
    String greeting;
}

var requestSchema = Schema.JSON(Request.class);
var responseSchema = Schema.JSON(Response.class);
```

### Client

```java
var configuration = RpcClientConfiguration.builder(requestSchema, responseSchema)
    .requestTopic("requests")
    .responseTopic("responses")
    .subscription("subscription")
    .build();

var client = RpcClient.create(pulsarClient, configuration);

var future = client.execute(new Request("Dave"));
```

### Server

```java
var serverConfiguration = RpcServerConfiguration.builder(requestSchema, responseSchema)
    .requestTopicsPattern(".+requests")
    .subscription("subscription")
    .build();

Function<Request, CompletableFuture<Response>> function =
    request -> completedFuture(new Response("Hello " + request.getValue() + "!"));

var server = RpcServer.create(pulsarClient, function, serverConfiguration);
```

