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

import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;

/**
 * Constructs messaging primitives used by the work scheduler system.
 *
 * @param <T> Type describing the task to be processed.
 */
@RequiredArgsConstructor
class MessagingFactory<T> {
  private final PulsarClient client;
  private final Schema<TaskMetadata> metadataSchema;
  private final TaskWorkerConfiguration<T, ?> configuration;

  TableView<TaskMetadata> taskMetadataTableView() throws PulsarClientException {
    return client
        .newTableViewBuilder(metadataSchema)
        .topic(configuration.getMetadataTopic())
        .create();
  }

  Producer<TaskMetadata> taskMetadataProducer() throws PulsarClientException {
    return client
        .newProducer(metadataSchema)
        .topic(configuration.getMetadataTopic())
        .enableBatching(false)
        .create();
  }

  Consumer<TaskMetadata> metadataEvictionConsumer(TaskMetadataEvictionListener evictionListener)
      throws PulsarClientException {
    return client
        .newConsumer(metadataSchema)
        .topic(configuration.getMetadataTopic())
        .subscriptionName(configuration.getSubscription())
        .subscriptionType(Shared)
        .enableRetry(true)
        .messageListener(evictionListener)
        .subscribe();
  }

  Consumer<T> taskConsumer(TaskListener<T, ?> taskListener) throws PulsarClientException {
    return client
        .newConsumer(configuration.getTaskSchema())
        .topic(configuration.getTaskTopic())
        .subscriptionName(configuration.getSubscription())
        .subscriptionType(Shared)
        .ackTimeout(configuration.getWorkerTaskTimeout().toMillis(), MILLISECONDS)
        .negativeAckRedeliveryDelay(configuration.getTaskRedeliveryDelay().toMillis(), MILLISECONDS)
        .receiverQueueSize(0)
        .messageListener(taskListener)
        .subscribe();
  }
}
