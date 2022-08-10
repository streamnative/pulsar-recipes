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
class TaskStateUpdater {
  private final Producer<TaskProcessingState> producer;

  void update(TaskProcessingState taskProcessingState) throws PulsarClientException {
    send(taskProcessingState, false);
    log.debug(
        "Updated state for {} to {}",
        taskProcessingState.getMessageId(),
        taskProcessingState.getState());
  }

  void delete(TaskProcessingState taskProcessingState) throws PulsarClientException {
    send(taskProcessingState, true);
    log.debug("Deleted state for {}", taskProcessingState.getMessageId());
  }

  private void send(TaskProcessingState taskProcessingState, boolean tombstone)
      throws PulsarClientException {
    producer
        .newMessage()
        .key(taskProcessingState.getMessageId())
        .value(tombstone ? null : taskProcessingState)
        .send();
  }
}
