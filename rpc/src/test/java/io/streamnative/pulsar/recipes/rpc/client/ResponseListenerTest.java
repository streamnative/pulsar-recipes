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

import static io.streamnative.pulsar.recipes.rpc.common.MessagePropertyKeys.ERROR_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponseListenerTest {
  private final Map<String, Context<String>> contexts = new HashMap<>();
  @Mock private Message<String> message;
  @Mock private CompletableFuture<String> response;
  @Mock private ScheduledFuture<?> timeout;
  private final String correlationId = "correlationId";
  private final String value = "response";
  private final String errorMessage = "errorMessage";
  private ResponseListener<String> responseListener;
  private Context<String> context;

  @BeforeEach
  void beforeEach() {
    responseListener = new ResponseListener<>(contexts);
    context = new Context<>(response, timeout);
    contexts.put(correlationId, context);
  }

  @Test
  void success() {
    when(message.getKey()).thenReturn(correlationId);
    when(message.getValue()).thenReturn(value);

    responseListener.onReceive(message);

    verify(timeout).cancel(false);
    verify(response).complete(value);
    assertThat(contexts.get(correlationId)).isNull();
  }

  @Test
  void failure() {
    when(message.getKey()).thenReturn(correlationId);
    when(message.getProperty(ERROR_MESSAGE)).thenReturn(errorMessage);

    responseListener.onReceive(message);

    verify(timeout).cancel(false);
    var captor = ArgumentCaptor.forClass(Exception.class);
    verify(response).completeExceptionally(captor.capture());
    assertThat(captor.getValue()).hasMessage(errorMessage);
    assertThat(contexts.get(correlationId)).isNull();
  }

  @Test
  void noContext() {
    contexts.clear();
    when(message.getKey()).thenReturn(correlationId);

    responseListener.onReceive(message);

    Mockito.verifyNoInteractions(timeout, response);
  }
}
