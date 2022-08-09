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

@RequiredArgsConstructor
class MessagingFactory<T> {
  private final PulsarClient client;
  private final Schema<ProcessingState> stateSchema;
  private final Configuration<T, ?> configuration;

  // TODO we're going to be consuming the state events twice - once for the tableView and once for
  // expiration
  TableView<ProcessingState> stateTableView() throws PulsarClientException {
    return client.newTableViewBuilder(stateSchema).topic(configuration.getStateTopic()).create();
  }

  Producer<ProcessingState> stateProducer() throws PulsarClientException {
    return client
        .newProducer(stateSchema)
        .topic(configuration.getStateTopic())
        .enableBatching(false)
        .create();
  }

  Consumer<ProcessingState> stateConsumer(ExpirationListener expirationListener)
      throws PulsarClientException {
    // TODO state consumer ackTimeout
    return client
        .newConsumer(stateSchema)
        .topic(configuration.getStateTopic())
        .subscriptionName(configuration.getSubscription())
        .subscriptionType(Shared)
        .negativeAckRedeliveryDelay(
            configuration.getExpirationRedeliveryDelay().toMillis(), MILLISECONDS)
        .messageListener(expirationListener)
        .subscribe();
  }

  Consumer<T> taskConsumer(TaskListener<T, ?> taskListener) throws PulsarClientException {
    // TODO task consumer ackTimeout
    return client
        .newConsumer(configuration.getTaskSchema())
        .topic(configuration.getTaskTopic())
        .subscriptionName(configuration.getSubscription())
        .subscriptionType(Shared)
        .negativeAckRedeliveryDelay(configuration.getTaskRedeliveryDelay().toMillis(), MILLISECONDS)
        .receiverQueueSize(0)
        .messageListener(taskListener)
        .subscribe();
  }
}
