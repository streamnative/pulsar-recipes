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
package io.streamnative.pulsar.recipes.rpc.common;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AutoAcknowledgeMessageListenerTest {
  @Mock private Consumer<String> consumer;
  @Mock private Message<String> message;
  @Spy private AutoAcknowledgeMessageListener<String> autoAcknowledgeMessageListener;

  @Test
  void receiveOk() throws Exception {
    autoAcknowledgeMessageListener.received(consumer, message);

    var inOrder = inOrder(autoAcknowledgeMessageListener, consumer);
    inOrder.verify(autoAcknowledgeMessageListener).onReceive(message);
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void receiveException() throws Exception {
    var receiveException = new RuntimeException();
    doThrow(receiveException).when(autoAcknowledgeMessageListener).onReceive(message);

    assertThatThrownBy(() -> autoAcknowledgeMessageListener.received(consumer, message))
        .isSameAs(receiveException);

    var inOrder = inOrder(autoAcknowledgeMessageListener, consumer);
    inOrder.verify(autoAcknowledgeMessageListener).onReceive(message);
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void receiveOkAcknowledgeException() throws Exception {
    var acknowledgeException = new PulsarClientException("");
    doThrow(acknowledgeException).when(consumer).acknowledge(message);

    autoAcknowledgeMessageListener.received(consumer, message);

    var inOrder = inOrder(autoAcknowledgeMessageListener, consumer);
    inOrder.verify(autoAcknowledgeMessageListener).onReceive(message);
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void receiveExceptionAcknowledgeException() throws Exception {
    var receiveException = new RuntimeException();
    var acknowledgeException = new PulsarClientException("");
    doThrow(receiveException).when(autoAcknowledgeMessageListener).onReceive(message);
    doThrow(acknowledgeException).when(consumer).acknowledge(message);

    assertThatThrownBy(() -> autoAcknowledgeMessageListener.received(consumer, message))
        .isSameAs(receiveException);

    var inOrder = inOrder(autoAcknowledgeMessageListener, consumer);
    inOrder.verify(autoAcknowledgeMessageListener).onReceive(message);
    inOrder.verify(consumer).acknowledge(message);
  }
}
