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
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Clock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Garbage collector for task metadata. Listens to metadata changes, and acts on those that describe
 * a task that is in a terminal state. Typically, all metadata updates will be ACKed immediately.
 * However, in the case of metadata describing completed/failed tasks, it may be scheduled for later
 * consumption so that a record of a task's processing is available for the retention period.
 */
@Slf4j
@RequiredArgsConstructor
class TaskMetadataEvictionListener implements MessageListener<TaskMetadata> {
  private final TaskMetadataUpdater metadataUpdater;
  private final Clock clock;
  private final int maxTaskAttempts;
  private final long terminalStateRetentionMillis;

  @Override
  public void received(
      Consumer<TaskMetadata> metadataConsumer, Message<TaskMetadata> metadataMessage) {
    try {
      TaskMetadata metadata = metadataMessage.getValue();
      log.debug("Received: {}", metadata);
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
      log.warn("Error while processing metadata for eviction", e);
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
      log.debug(
          "Task metadata not yet eligible for eviction - delaying redelivery until eviction horizon: {}",
          metadata);
      metadataConsumer.reconsumeLater(
          metadataMessage, intervalUntilMetadataEviction(metadata), MILLISECONDS);
    } else {
      log.debug("Evicting task metadata: {}", metadata);
      metadataUpdater.delete(metadata);
      metadataConsumer.acknowledge(metadataMessage);
    }
  }

  private long intervalUntilMetadataEviction(TaskMetadata metadata) {
    long evictionTimestamp = metadata.getLastUpdated() + terminalStateRetentionMillis;
    return max(0L, evictionTimestamp - clock.millis());
  }
}
