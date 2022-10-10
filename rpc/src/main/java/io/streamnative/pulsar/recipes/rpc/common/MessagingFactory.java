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

import java.io.IOException;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

@Slf4j
@RequiredArgsConstructor
public class MessagingFactory<REQUEST, RESPONSE> {
  private final PulsarClient client;
  private final Schema<REQUEST> requestSchema;
  private final Schema<RESPONSE> responseSchema;
  private final String subscription;

  public Producer<REQUEST> requestProducer(String topic) throws IOException {
    return client
        .newProducer(requestSchema)
        .topic(topic)
        // only a single client per channel
        .accessMode(ProducerAccessMode.Exclusive)
        .create();
  }

  public Consumer<RESPONSE> responseConsumer(String topic, MessageListener<RESPONSE> listener)
      throws IOException {
    return client
        .newConsumer(responseSchema)
        .topic(topic)
        .subscriptionName(subscription)
        // only a single client per channel
        .subscriptionType(SubscriptionType.Exclusive)
        .messageListener(listener)
        .subscribe();
  }

  public Producer<RESPONSE> responseProducer(String topic) throws IOException {
    return client
        .newProducer(responseSchema)
        .topic(topic)
        // multiple servers can respond to a channel
        .accessMode(ProducerAccessMode.Shared)
        .create();
  }

  public Consumer<REQUEST> requestConsumer(
      String topicsPattern, Duration channelDiscoveryInterval, MessageListener<REQUEST> listener)
      throws IOException {
    return client
        .newConsumer(requestSchema)
        .topicsPattern(topicsPattern)
        // patternAutoDiscoveryPeriod and subscriptionInitialPosition must be set to start consuming
        // from newly created channel request topics within good time and to read from earliest to
        // pick up the first requests
        .patternAutoDiscoveryPeriod((int) channelDiscoveryInterval.toMillis(), MILLISECONDS)
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
        .subscriptionName(subscription)
        // servers can be horizontally scaled but the same server should receive
        // all messages for the same request
        // Multiple responses per request need to be Key_Shared, but single message
        // requests/response can just be Shared
        .subscriptionType(SubscriptionType.Key_Shared)
        .messageListener(listener)
        .subscribe();
  }
}
