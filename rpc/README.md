# RPC

A distributed RPC framework. A limitation of traditional RPC frameworks is that they require a direct connection between
client and server. This system provides additional fault tolerance by using Apache Pulsar for the transport which allows
the direct interaction between client and server to be decoupled.

## Methodology

* Calls are communicated over a channel.
* A channel consists of a request topic and a response topic.
* A client sends requests on a single request topic and listens for responses on a single response topic.
* A server listens for requests on multiple request topics and sends the responses on the appropriate response topic
  for each request.
* The client tells the server which topic to respond on in each request.
* The client identifies which request a response is for by attaching a correlation id to each request.
* The server likewise attaches the correlation id to each response.

## Configuration

### Topic naming

An RPC server needs to listen for requests on potentially many request topics including those that may not exist when
the listener is created. In Pulsar this is achieved by using a regular expression pattern to match potential topic
names. Consideration must be given to match only request topics, being careful not to subscribe to response topics and
non-RPC topics. The simplest approach to this would be to scope RPC to a specific Pulsar namespace to eliminate non-RPC
topics and to suffix request and response topics appropriately.

e.g.

| Topic     | Name                   |
|-----------|------------------------|
| Requests  | `${channel}-requests`  |
| Responses | `${channel}-responses` |


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
    .requestTopic("rpc-requests")
    .responseTopic("rpc-responses")
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

