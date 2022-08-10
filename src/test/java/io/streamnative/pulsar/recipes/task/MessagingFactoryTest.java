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
package io.streamnative.pulsar.recipes.task;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.pulsar.client.api.SubscriptionType.Shared;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MessagingFactoryTest {
  @Mock private PulsarClient client;
  private final Schema<TaskProcessingState> stateSchema = Schema.JSON(TaskProcessingState.class);
  private final TaskWorkerConfiguration<String, String> configuration =
      TaskWorkerConfiguration.builder(Schema.STRING, Schema.STRING)
          .taskTopic("tasks")
          .subscription("subscription")
          .retention(Duration.ofSeconds(1))
          .expirationRedeliveryDelay(Duration.ofSeconds(1))
          .build();
  private MessagingFactory<String> messagingFactory;

  @BeforeEach
  void beforeEach() {
    messagingFactory = new MessagingFactory<>(client, stateSchema, configuration);
  }

  @Test
  void stateTableView(
      @Mock TableViewBuilder<TaskProcessingState> builder,
      @Mock TableView<TaskProcessingState> tableView)
      throws PulsarClientException {
    when(client.newTableViewBuilder(stateSchema)).thenReturn(builder);
    when(builder.topic(configuration.getStateTopic())).thenReturn(builder);
    when(builder.create()).thenReturn(tableView);

    TableView<TaskProcessingState> result = messagingFactory.taskStateTableView();

    assertThat(result).isSameAs(tableView);
  }

  @Test
  void stateProducer(
      @Mock ProducerBuilder<TaskProcessingState> builder,
      @Mock Producer<TaskProcessingState> producer)
      throws PulsarClientException {
    when(client.newProducer(stateSchema)).thenReturn(builder);
    when(builder.topic(configuration.getStateTopic())).thenReturn(builder);
    when(builder.enableBatching(false)).thenReturn(builder);
    when(builder.create()).thenReturn(producer);

    Producer<TaskProcessingState> result = messagingFactory.taskStateProducer();

    assertThat(result).isSameAs(producer);
  }

  @Test
  void stateConsumer(
      @Mock ConsumerBuilder<TaskProcessingState> builder,
      @Mock ExpirationListener listener,
      @Mock Consumer<TaskProcessingState> consumer)
      throws PulsarClientException {
    when(client.newConsumer(stateSchema)).thenReturn(builder);
    when(builder.topic(configuration.getStateTopic())).thenReturn(builder);
    when(builder.subscriptionName(configuration.getSubscription())).thenReturn(builder);
    when(builder.subscriptionType(Shared)).thenReturn(builder);
    when(builder.negativeAckRedeliveryDelay(
            configuration.getExpirationRedeliveryDelay().toMillis(), MILLISECONDS))
        .thenReturn(builder);
    when(builder.messageListener(listener)).thenReturn(builder);
    when(builder.subscribe()).thenReturn(consumer);

    Consumer<TaskProcessingState> result = messagingFactory.taskStateConsumer(listener);

    assertThat(result).isSameAs(consumer);
  }

  @Test
  void taskConsumer(
      @Mock ConsumerBuilder<String> builder,
      @Mock TaskListener<String, String> listener,
      @Mock Consumer<String> consumer)
      throws PulsarClientException {
    when(client.newConsumer(configuration.getTaskSchema())).thenReturn(builder);
    when(builder.topic(configuration.getTaskTopic())).thenReturn(builder);
    when(builder.subscriptionName(configuration.getSubscription())).thenReturn(builder);
    when(builder.subscriptionType(Shared)).thenReturn(builder);
    when(builder.negativeAckRedeliveryDelay(
            configuration.getTaskRedeliveryDelay().toMillis(), MILLISECONDS))
        .thenReturn(builder);
    when(builder.receiverQueueSize(0)).thenReturn(builder);
    when(builder.messageListener(listener)).thenReturn(builder);
    when(builder.subscribe()).thenReturn(consumer);

    Consumer<String> result = messagingFactory.taskConsumer(listener);

    assertThat(result).isSameAs(consumer);
  }
}
