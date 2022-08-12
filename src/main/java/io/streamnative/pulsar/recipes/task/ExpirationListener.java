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

@Slf4j
@RequiredArgsConstructor
class ExpirationListener implements MessageListener<TaskProcessingState> {
  private final TaskStateUpdater taskStateUpdater;
  private final Clock clock;
  private final int maxAttempts;
  private final long retentionMillis;

  @Override
  public void received(
      Consumer<TaskProcessingState> consumer, Message<TaskProcessingState> message) {
    log.debug("Received: {}", message.getValue());
    try {
      TaskProcessingState taskProcessingState = message.getValue();
      if (taskProcessingState == null) { // tombstone
        consumer.acknowledge(message);
      } else {
        switch (taskProcessingState.getState()) {
          case COMPLETED:
            checkExpiration(consumer, message);
            break;
          case FAILED:
            if (taskProcessingState.getAttempts() == maxAttempts) {
              checkExpiration(consumer, message);
            } else {
              consumer.acknowledge(message);
            }
            break;
          case NEW:
          case PROCESSING:
          default:
            consumer.acknowledge(message);
            break;
        }
      }
    } catch (PulsarClientException e) {
      log.warn("Error while performing expiry", e);
    }
  }

  private void checkExpiration(
      Consumer<TaskProcessingState> consumer, Message<TaskProcessingState> message)
      throws PulsarClientException {
    TaskProcessingState taskProcessingState = message.getValue();
    long expiryTimestamp = taskProcessingState.getLastUpdated() + retentionMillis;
    long expiryInterval = expiryTimestamp - clock.millis();
    log.debug("State expires in {} milliseconds", expiryInterval); // TODO debug -> trace
    if (expiryInterval > 0) {
      log.debug("State not yet expired: {}", taskProcessingState);
      // TODO would be nice to be able to redeliver only once at exactly the right time with reconsumeLater
      // See also:
      // Currently, retry letter topic is enabled in Shared subscription types.
      // Compared with negative acknowledgment, retry letter topic is more suitable for messages that require a large
      // number of retries with a configurable retry interval. Because messages in the retry letter topic are
      // persisted to BookKeeper, while messages that need to be retried due to negative acknowledgment are cached on
      // the client side.
      // consumer.reconsumeLater(stateMessage, max(0L, expiryTimestamp - clock.millis()), MILLISECONDS);
      consumer.negativeAcknowledge(message);
    } else {
      log.debug("Deleting state: {}", taskProcessingState);
      taskStateUpdater.delete(taskProcessingState);
      consumer.acknowledge(message);
    }
  }
}
