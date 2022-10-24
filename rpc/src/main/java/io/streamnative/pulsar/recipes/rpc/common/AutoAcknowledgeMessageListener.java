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


import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
public abstract class AutoAcknowledgeMessageListener<T> implements MessageListener<T> {
  @Override
  public void received(Consumer<T> consumer, Message<T> message) {
    try {
      onReceive(message);
    } finally {
      try {
        consumer.acknowledge(message);
      } catch (PulsarClientException e) {
        log.warn(
            "Error acknowledging message {} on topic {}",
            message.getMessageId(),
            message.getTopicName(),
            e);
      }
    }
  }

  protected abstract void onReceive(Message<T> message);
}
