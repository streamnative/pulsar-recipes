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

import static io.streamnative.pulsar.recipes.rpc.common.RpcConstants.RESPONSE_TOPIC;
import static io.streamnative.pulsar.recipes.rpc.common.TestUtils.mockTypedMessageBuilder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RequestSenderTest {
  @Mock private Producer<String> producer;
  private final String responseTopic = "responseTopic";
  private final String correlationId = "correlationId";
  private final String request = "request";
  private final TypedMessageBuilder<String> typedMessageBuilder = mockTypedMessageBuilder();
  private RequestSender<String> requestSender;

  @BeforeEach
  void beforeEach() {
    requestSender = new RequestSender<>(producer, responseTopic);
  }

  @Test
  void test() throws Exception {
    when(producer.newMessage()).thenReturn(typedMessageBuilder);

    requestSender.send(correlationId, request);

    verify(typedMessageBuilder).property(RESPONSE_TOPIC, responseTopic);
    verify(typedMessageBuilder).key(correlationId);
    verify(typedMessageBuilder).value(request);
    verify(typedMessageBuilder).send();
  }
}
