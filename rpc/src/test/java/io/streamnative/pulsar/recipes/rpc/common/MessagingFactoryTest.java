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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MessagingFactoryTest {
  @Mock private PulsarClient client;
  @Mock private Schema<String> requestSchema;
  @Mock private Schema<String> responseSchema;
  private final String subscription = "subscription";
  private final String topic = "topic";
  private MessagingFactory<String, String> messagingFactory;

  @BeforeEach
  void beforeEach() {
    messagingFactory = new MessagingFactory<>(client, requestSchema, responseSchema, subscription);
  }

  @Test
  void requestProducer(@Mock Producer<String> producer) throws Exception {
    var producerBuilder = TestUtils.<String>mockProducerBuilder();
    when(client.newProducer(requestSchema)).thenReturn(producerBuilder);
    when(producerBuilder.create()).thenReturn(producer);

    assertThat(messagingFactory.requestProducer(topic)).isSameAs(producer);

    verify(producerBuilder).topic(topic);
    verify(producerBuilder).accessMode(ProducerAccessMode.Exclusive);
    verifyNoMoreInteractions(producerBuilder);
  }

  @Test
  void responseConsumer(@Mock Consumer<String> consumer, @Mock MessageListener<String> listener)
      throws Exception {
    var consumerBuilder = TestUtils.<String>mockConsumerBuilder();
    when(client.newConsumer(responseSchema)).thenReturn(consumerBuilder);
    when(consumerBuilder.subscribe()).thenReturn(consumer);

    assertThat(messagingFactory.responseConsumer(topic, listener)).isSameAs(consumer);

    verify(consumerBuilder).topic(topic);
    verify(consumerBuilder).subscriptionName(subscription);
    verify(consumerBuilder).subscriptionType(SubscriptionType.Exclusive);
    verify(consumerBuilder).messageListener(listener);
    verifyNoMoreInteractions(consumerBuilder);
  }

  @Test
  void responseProducer(@Mock Producer<String> producer) throws Exception {
    var producerBuilder = TestUtils.<String>mockProducerBuilder();
    when(client.newProducer(responseSchema)).thenReturn(producerBuilder);
    when(producerBuilder.create()).thenReturn(producer);

    assertThat(messagingFactory.responseProducer(topic)).isSameAs(producer);

    verify(producerBuilder).topic(topic);
    verify(producerBuilder).accessMode(ProducerAccessMode.Shared);
    verifyNoMoreInteractions(producerBuilder);
  }

  @Test
  void requestConsumer(@Mock Consumer<String> consumer, @Mock MessageListener<String> listener)
      throws Exception {
    var consumerBuilder = TestUtils.<String>mockConsumerBuilder();
    when(client.newConsumer(requestSchema)).thenReturn(consumerBuilder);
    when(consumerBuilder.subscribe()).thenReturn(consumer);

    assertThat(messagingFactory.requestConsumer(topic, Duration.ofSeconds(1), listener))
        .isSameAs(consumer);

    verify(consumerBuilder).topicsPattern(topic);
    verify(consumerBuilder).subscriptionName(subscription);
    verify(consumerBuilder).patternAutoDiscoveryPeriod(1000, MILLISECONDS);
    verify(consumerBuilder).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
    verify(consumerBuilder).subscriptionType(SubscriptionType.Key_Shared);
    verify(consumerBuilder).messageListener(listener);
    verifyNoMoreInteractions(consumerBuilder);
  }
}
