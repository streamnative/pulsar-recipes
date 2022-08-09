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


import java.time.Clock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
@RequiredArgsConstructor
class ExpirationListener implements MessageListener<ProcessingState> {
  private final StateUpdater stateUpdater;
  private final Clock clock;
  private final int maxAttempts;
  private final long retentionMillis;

  @Override
  public void received(Consumer<ProcessingState> consumer, Message<ProcessingState> message) {
    log.debug("Received: {}", message.getValue());
    try {
      ProcessingState processingState = message.getValue();
      if (processingState == null) { // tombstone
        consumer.acknowledge(message);
      } else {
        switch (processingState.getState()) {
          case COMPLETED:
            checkExpiration(consumer, message);
            break;
          case FAILED:
            if (processingState.getAttempts() == maxAttempts) {
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

  private void checkExpiration(Consumer<ProcessingState> consumer, Message<ProcessingState> message)
      throws PulsarClientException {
    ProcessingState processingState = message.getValue();
    long expiryTimestamp = processingState.getLastUpdated() + retentionMillis;
    long expiryInterval = expiryTimestamp - clock.millis();
    log.debug("State expires in {} milliseconds", expiryInterval); // TOD debug -> trace
    if (expiryInterval > 0) {
      log.debug("State not yet expired: {}", processingState);
      // TODO would be nice to be able to redeliver only once at exactly the right time with
      // redeliverLater
      consumer.negativeAcknowledge(message);
    } else {
      log.debug("Deleting state: {}", processingState);
      stateUpdater.delete(processingState);
      consumer.acknowledge(message);
    }
  }
}
