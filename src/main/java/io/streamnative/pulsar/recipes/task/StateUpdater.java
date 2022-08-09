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


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
@RequiredArgsConstructor
class StateUpdater {
  private final Producer<ProcessingState> producer;

  void update(ProcessingState processingState) throws PulsarClientException {
    send(processingState, false);
    log.debug(
        "Updated state for {} to {}", processingState.getMessageId(), processingState.getState());
  }

  void delete(ProcessingState processingState) throws PulsarClientException {
    send(processingState, true);
    log.debug("Deleted state for {}", processingState.getMessageId());
  }

  private void send(ProcessingState processingState, boolean tombstone)
      throws PulsarClientException {
    producer
        .newMessage()
        .key(processingState.getMessageId())
        .value(tombstone ? null : processingState)
        .send();
  }
}
