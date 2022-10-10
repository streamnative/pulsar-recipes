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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RpcClientTest {
  private final Map<String, Context<String>> contexts = new HashMap<>();
  private final Duration responseTimeout = Duration.ofSeconds(1);
  private final String request = "request";
  private final String correlationId = "correlationId";
  @Mock private Supplier<String> correlationIdSupplier;
  @Mock private ScheduledExecutorService executor;
  @Mock private RequestSender<String> sender;
  @Mock private Producer<?> requestProducer;
  @Mock private Consumer<?> responseConsumer;

  private RpcClient<String, String> client;

  @BeforeEach
  void beforeEach() {
    client =
        new RpcClient<>(
            correlationIdSupplier,
            contexts,
            responseTimeout,
            executor,
            sender,
            requestProducer,
            responseConsumer);
  }

  @Test
  void success() throws Exception {
    when(correlationIdSupplier.get()).thenReturn(correlationId);

    var future = client.execute(request);

    verify(executor)
        .schedule(any(Runnable.class), eq(responseTimeout.toMillis()), eq(MILLISECONDS));

    assertThat(contexts.size()).isEqualTo(1);
    var context = contexts.get(correlationId);

    verify(sender).send(correlationId, request);

    assertThat(future).isSameAs(context.getResponse());
  }

  @Test
  void sendFail() throws Exception {
    when(correlationIdSupplier.get()).thenReturn(correlationId);

    var exception = new IOException();
    doThrow(exception).when(sender).send(correlationId, request);

    var timeoutFuture = mock(ScheduledFuture.class);
    doReturn(timeoutFuture).when(executor).schedule(any(Runnable.class), anyLong(), any());

    var future = client.execute(request);

    var timeoutCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(executor)
        .schedule(timeoutCaptor.capture(), eq(responseTimeout.toMillis()), eq(MILLISECONDS));

    assertThat(contexts.size()).isEqualTo(0);

    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThatThrownBy(future::get)
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IOException.class);

    verify(timeoutFuture).cancel(true);
  }

  @Test
  void timeout() {
    when(correlationIdSupplier.get()).thenReturn(correlationId);

    var timeoutFuture = mock(ScheduledFuture.class);
    doReturn(timeoutFuture).when(executor).schedule(any(Runnable.class), anyLong(), any());

    var future = client.execute(request);

    var timeoutCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(executor)
        .schedule(timeoutCaptor.capture(), eq(responseTimeout.toMillis()), eq(MILLISECONDS));

    timeoutCaptor.getValue().run();

    assertThat(contexts.size()).isEqualTo(0);

    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThatThrownBy(future::get)
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(TimeoutException.class);
  }

  @Test
  void close() throws Exception {
    client.close();

    verify(requestProducer).close();
    verify(responseConsumer).close();
  }
}
