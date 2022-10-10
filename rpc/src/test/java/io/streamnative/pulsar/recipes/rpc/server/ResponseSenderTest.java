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

import static io.streamnative.pulsar.recipes.rpc.common.RpcConstants.ERROR_MESSAGE;
import static io.streamnative.pulsar.recipes.rpc.common.TestUtils.mockTypedMessageBuilder;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponseSenderTest {
  @Mock private KeyedObjectPool<String, Producer<String>> pool;
  @Mock private Producer<String> producer;
  private final TypedMessageBuilder<String> typedMessageBuilder = mockTypedMessageBuilder();
  private final String topic = "topic";
  private final String correlationId = "correlationId";
  private final String response = "response";
  private final String errorMessage = "errorMessage";
  private ResponseSender<String> sender;

  @BeforeEach
  void beforeEach() {
    sender = new ResponseSender(pool);
  }

  @Test
  void sendResponse() throws Exception {
    when(pool.borrowObject(topic)).thenReturn(producer);
    when(producer.newMessage()).thenReturn(typedMessageBuilder);

    sender.sendResponse(topic, correlationId, response);

    var inOrder = inOrder(pool, producer, typedMessageBuilder);
    inOrder.verify(pool).borrowObject(topic);
    inOrder.verify(producer).newMessage();
    inOrder.verify(typedMessageBuilder).key(correlationId);
    inOrder.verify(typedMessageBuilder).value(response);
    inOrder.verify(typedMessageBuilder).send();
    inOrder.verify(pool).returnObject(topic, producer);
  }

  @Test
  void sendError() throws Exception {
    when(pool.borrowObject(topic)).thenReturn(producer);
    when(producer.newMessage()).thenReturn(typedMessageBuilder);

    sender.sendError(topic, correlationId, errorMessage);

    var inOrder = inOrder(pool, producer, typedMessageBuilder);
    inOrder.verify(pool).borrowObject(topic);
    inOrder.verify(producer).newMessage();
    inOrder.verify(typedMessageBuilder).key(correlationId);
    inOrder.verify(typedMessageBuilder).property(ERROR_MESSAGE, errorMessage);
    inOrder.verify(typedMessageBuilder).value(null);
    inOrder.verify(typedMessageBuilder).send();
    inOrder.verify(pool).returnObject(topic, producer);
  }
}
