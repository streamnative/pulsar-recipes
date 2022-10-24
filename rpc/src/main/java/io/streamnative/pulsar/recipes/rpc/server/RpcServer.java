/*
 * Copyright © 2022 StreamNative
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.recipes.rpc.server;

import static lombok.AccessLevel.PACKAGE;

import io.streamnative.pulsar.recipes.rpc.common.MessagingFactory;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;

/** Responsible for the processing of requests. */
@RequiredArgsConstructor(access = PACKAGE)
public class RpcServer implements AutoCloseable {
  private final Consumer<?> requestConsumer;
  private final GenericKeyedObjectPool<?, ?> responseProducerPool;

  /**
   * Creates a new RPC server.
   *
   * @param client the Pulsar client
   * @param function the function that processes requests.
   * @param configuration the server configuration
   * @return the RPC server
   * @param <REQUEST> Type describing the request to be processed.
   * @param <RESPONSE> Response type yielded from the processing of the request.
   * @throws IOException
   */
  public static <REQUEST, RESPONSE> RpcServer create(
      @NonNull PulsarClient client,
      @NonNull Function<REQUEST, CompletableFuture<RESPONSE>> function,
      @NonNull RpcServerConfiguration<REQUEST, RESPONSE> configuration)
      throws IOException {
    var messagingFactory =
        new MessagingFactory<>(
            client,
            configuration.getRequestSchema(),
            configuration.getResponseSchema(),
            configuration.getSubscription());
    var poolFactory = new ResponseProducerPoolFactory<>(messagingFactory);
    var pool = new GenericKeyedObjectPool<>(poolFactory);
    var sender = new ResponseSender<>(pool);
    var listener = new RequestListener<>(function, sender, configuration.getResponseTimeout());
    var consumer =
        messagingFactory.requestConsumer(
            configuration.getRequestTopicsPattern(),
            configuration.getChannelDiscoveryInterval(),
            listener);
    return new RpcServer(consumer, pool);
  }

  @Override
  public void close() throws Exception {
    try (requestConsumer;
        responseProducerPool) {}
  }
}
