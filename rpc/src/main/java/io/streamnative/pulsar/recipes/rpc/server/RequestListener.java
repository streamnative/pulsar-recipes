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

import static io.streamnative.pulsar.recipes.rpc.common.MessagePropertyKeys.RESPONSE_TOPIC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.streamnative.pulsar.recipes.rpc.common.AutoAcknowledgeMessageListener;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;

@Slf4j
@RequiredArgsConstructor
class RequestListener<REQUEST, RESPONSE> extends AutoAcknowledgeMessageListener<REQUEST> {
  private final Function<REQUEST, CompletableFuture<RESPONSE>> function;
  private final ResponseSender<RESPONSE> sender;
  private final Duration responseTimeout;

  @Override
  protected void onReceive(Message<REQUEST> message) {
    var correlationId = message.getKey();
    log.debug("Received {}", correlationId);

    var responseTopic = message.getProperty(RESPONSE_TOPIC);
    var request = message.getValue();

    try {
      function
          .apply(request)
          .orTimeout(responseTimeout.toMillis(), MILLISECONDS)
          .thenAccept(response -> sender.sendResponse(responseTopic, correlationId, response))
          .get();
    } catch (ExecutionException e) {
      log.error("Error processing request", e);
      var cause = e.getCause();
      sender.sendError(
          responseTopic, correlationId, cause.getClass().getName() + ": " + cause.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
