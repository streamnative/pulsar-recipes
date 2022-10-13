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
package io.streamnative.pulsar.recipes.rpc.client;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.pulsar.recipes.rpc.common.MessagingFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * Responsible for the processing of requests.
 *
 * @param <REQUEST> Type describing the request to be processed.
 * @param <RESPONSE> Response type yielded from the processing of the request.
 */
@RequiredArgsConstructor(access = PACKAGE)
public class RpcClient<REQUEST, RESPONSE> implements AutoCloseable {
  private final Supplier<String> correlationIdSupplier;
  private final Map<String, Context<RESPONSE>> contexts;
  private final Duration responseTimeout;
  private final ScheduledExecutorService executor;
  private final RequestSender<REQUEST> sender;
  private final Producer<?> requestProducer;
  private final Consumer<?> responseConsumer;

  /**
   * Creates a new RPC client.
   *
   * <p>A single client instance can be used across multiple threads.
   *
   * @param client the Pulsar client
   * @param configuration the client configuration
   * @return the RPC client
   * @param <REQUEST> Type describing the request to be processed.
   * @param <RESPONSE> Response type yielded from the processing of the request.
   * @throws IOException
   */
  public static <REQUEST, RESPONSE> RpcClient<REQUEST, RESPONSE> create(
      @NonNull PulsarClient client, RpcClientConfiguration<REQUEST, RESPONSE> configuration)
      throws IOException {
    Supplier<String> correlationIdSupplier = () -> randomUUID().toString();
    var contexts = new HashMap<String, Context<RESPONSE>>();
    var executor = Executors.newSingleThreadScheduledExecutor();
    var messagingFactory =
        new MessagingFactory<>(
            client,
            configuration.getRequestSchema(),
            configuration.getResponseSchema(),
            configuration.getSubscription());
    var producer = messagingFactory.requestProducer(configuration.getRequestTopic());
    var sender = new RequestSender<>(producer, configuration.getResponseTopic());
    var listener = new ResponseListener<>(contexts);
    var consumer = messagingFactory.responseConsumer(configuration.getResponseTopic(), listener);
    return new RpcClient<>(
        correlationIdSupplier,
        contexts,
        configuration.getResponseTimeout(),
        executor,
        sender,
        producer,
        consumer);
  }

  public CompletableFuture<RESPONSE> execute(REQUEST request) {
    var responseFuture = new CompletableFuture<RESPONSE>();
    var correlationId = correlationIdSupplier.get();

    var timeoutFuture =
        executor.schedule(() -> onTimeout(correlationId), responseTimeout.toMillis(), MILLISECONDS);

    var context = new Context<>(responseFuture, timeoutFuture);
    contexts.put(correlationId, context);

    try {
      sender.send(correlationId, request);
    } catch (IOException e) {
      contexts.remove(correlationId);
      timeoutFuture.cancel(true);
      responseFuture.completeExceptionally(e);
    }

    return responseFuture;
  }

  private void onTimeout(String correlationId) {
    var context = contexts.remove(correlationId);
    if (context != null) {
      context
          .getResponse()
          .completeExceptionally(
              new TimeoutException("Request exceeded timeout [" + responseTimeout + "]"));
    }
  }

  @Override
  public void close() throws Exception {
    try (responseConsumer;
        requestProducer) {}
  }
}
