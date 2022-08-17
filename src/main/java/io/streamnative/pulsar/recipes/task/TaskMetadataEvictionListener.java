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

import static java.lang.Math.max;

import java.time.Clock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
@RequiredArgsConstructor
class TaskMetadataEvictionListener implements MessageListener<TaskMetadata> {
  private final TaskMetadataUpdater stateUpdater;
  private final Clock clock;
  private final int maxTaskAttempts;
  private final long terminalStateRetentionMillis;

  @Override
  public void received(
      Consumer<TaskMetadata> metadataConsumer, Message<TaskMetadata> metadataMessage) {
    log.debug("Received: {}", metadataMessage.getValue());
    try {
      TaskMetadata metadata = metadataMessage.getValue();
      if (metadata == null) { // tombstone
        metadataConsumer.acknowledge(metadataMessage);
      } else {
        switch (metadata.getState()) {
          case COMPLETED:
            checkTerminalTaskMetadata(metadataConsumer, metadataMessage);
            break;
          case FAILED:
            if (metadata.getAttempts() == maxTaskAttempts) {
              checkTerminalTaskMetadata(metadataConsumer, metadataMessage);
            } else {
              metadataConsumer.acknowledge(metadataMessage);
            }
            break;
          case NEW:
          case PROCESSING:
          default:
            metadataConsumer.acknowledge(metadataMessage);
            break;
        }
      }
    } catch (PulsarClientException e) {
      log.warn("Error while performing expiry", e);
    }
  }

  private void checkTerminalTaskMetadata(
      Consumer<TaskMetadata> metadataConsumer, Message<TaskMetadata> metadataMessage)
      throws PulsarClientException {
    TaskMetadata metadata = metadataMessage.getValue();
    long intervalUntilMetadataEviction = intervalUntilMetadataEviction(metadata);
    // TODO debug -> trace
    log.debug("Task metadata should be evicted in {} milliseconds", intervalUntilMetadataEviction);
    if (intervalUntilMetadataEviction > 0) {
      log.debug("Task metadata not yet eligible for eviction: {}", metadata);
      // TODO would be nice to be able to redeliver only once at exactly the right time with
      // reconsumeLater
      // See also:
      // Currently, retry letter topic is enabled in Shared subscription types.
      // Compared with negative acknowledgment, retry letter topic is more suitable for messages
      // that require a large
      // number of retries with a configurable retry interval. Because messages in the retry letter
      // topic are
      // persisted to BookKeeper, while messages that need to be retried due to negative
      // acknowledgment are cached on
      // the client side.
      // consumer.reconsumeLater(metadataMessage, intervalUntilMetadataEviction(metadata),
      // MILLISECONDS);
      metadataConsumer.negativeAcknowledge(metadataMessage);
    } else {
      log.debug("Evicting task metadata: {}", metadata);
      stateUpdater.delete(metadata);
      metadataConsumer.acknowledge(metadataMessage);
    }
  }

  private long intervalUntilMetadataEviction(TaskMetadata metadata) {
    long evictionTimestamp = metadata.getLastUpdated() + terminalStateRetentionMillis;
    return max(0L, evictionTimestamp - clock.millis());
  }
}
