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
import static java.util.concurrent.TimeUnit.SECONDS;
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
  private final Schema<TaskMetadata> metadataSchema = Schema.JSON(TaskMetadata.class);
  private final TaskWorkerConfiguration<String, String> configuration =
      TaskWorkerConfiguration.builder(Schema.STRING, Schema.STRING)
          .taskTopic("tasks")
          .subscription("subscription")
          .retention(Duration.ofSeconds(1))
          //          .expirationRedeliveryDelay(Duration.ofSeconds(1))
          .build();
  private MessagingFactory<String> messagingFactory;

  @BeforeEach
  void beforeEach() {
    messagingFactory = new MessagingFactory<>(client, metadataSchema, configuration);
  }

  @Test
  void metadataTableView(
      @Mock TableViewBuilder<TaskMetadata> builder, @Mock TableView<TaskMetadata> tableView)
      throws PulsarClientException {
    when(client.newTableViewBuilder(metadataSchema)).thenReturn(builder);
    when(builder.topic(configuration.getMetadataTopic())).thenReturn(builder);
    when(builder.create()).thenReturn(tableView);

    TableView<TaskMetadata> result = messagingFactory.taskMetadataTableView();

    assertThat(result).isSameAs(tableView);
  }

  @Test
  void metadataProducer(
      @Mock ProducerBuilder<TaskMetadata> builder, @Mock Producer<TaskMetadata> producer)
      throws PulsarClientException {
    when(client.newProducer(metadataSchema)).thenReturn(builder);
    when(builder.topic(configuration.getMetadataTopic())).thenReturn(builder);
    when(builder.enableBatching(false)).thenReturn(builder);
    when(builder.create()).thenReturn(producer);

    Producer<TaskMetadata> result = messagingFactory.taskMetadataProducer();

    assertThat(result).isSameAs(producer);
  }

  @Test
  void metadataConsumer(
      @Mock ConsumerBuilder<TaskMetadata> builder,
      @Mock TaskMetadataEvictionListener listener,
      @Mock Consumer<TaskMetadata> consumer)
      throws PulsarClientException {
    when(client.newConsumer(metadataSchema)).thenReturn(builder);
    when(builder.topic(configuration.getMetadataTopic())).thenReturn(builder);
    when(builder.subscriptionName(configuration.getSubscription())).thenReturn(builder);
    when(builder.subscriptionType(Shared)).thenReturn(builder);
    when(builder.enableRetry(true)).thenReturn(builder);
    when(builder.ackTimeout(1, SECONDS)).thenReturn(builder);
    when(builder.messageListener(listener)).thenReturn(builder);
    when(builder.subscribe()).thenReturn(consumer);

    Consumer<TaskMetadata> result = messagingFactory.metadataEvictionConsumer(listener);

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
