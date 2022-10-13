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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RequestListenerTest {
  @Mock private Function<String, CompletableFuture<String>> function;
  @Mock private ResponseSender<String> sender;
  private final Duration responseTimeout = Duration.ofSeconds(1);
  @Mock private Message<String> message;
  private final String correlationId = "correlationId";
  private final String responseTopic = "responseTopic";
  private final String request = "request";
  private final String response = "response";
  private RequestListener<String, String> requestListener;

  @BeforeEach
  void beforeEach() {
    requestListener = new RequestListener<>(function, sender, responseTimeout);
    when(message.getKey()).thenReturn(correlationId);
    when(message.getProperty(RESPONSE_TOPIC)).thenReturn(responseTopic);
    when(message.getValue()).thenReturn(request);
  }

  @Test
  void success() {
    when(function.apply(request)).thenReturn(completedFuture(response));

    requestListener.onReceive(message);

    verify(sender).sendResponse(responseTopic, correlationId, response);
  }

  @Test
  void timeout() {
    var executor = delayedExecutor(2, SECONDS);
    when(function.apply(request)).thenReturn(supplyAsync(() -> response, executor));

    requestListener.onReceive(message);

    verify(sender)
        .sendError(responseTopic, correlationId, "java.util.concurrent.TimeoutException: null");
  }

  @Test
  void exception() {
    var exception = new RuntimeException("error");
    when(function.apply(request)).thenReturn(failedFuture(exception));

    requestListener.onReceive(message);

    verify(sender).sendError(responseTopic, correlationId, "java.lang.RuntimeException: error");
  }

  @Test
  void interrupted() {
    var executor = delayedExecutor(2, SECONDS);
    when(function.apply(request)).thenReturn(supplyAsync(() -> response, executor));

    Thread.currentThread().interrupt();
    assertThatThrownBy(() -> requestListener.onReceive(message))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(InterruptedException.class);

    assertThat(Thread.interrupted()).isTrue();

    verifyNoInteractions(sender);
  }
}
