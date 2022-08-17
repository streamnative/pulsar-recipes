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
class TaskMetadataUpdater {
  private final Producer<TaskMetadata> producer;

  void update(TaskMetadata metadata) throws PulsarClientException {
    send(metadata, false);
    log.debug("Updated metadata for {} to {}", metadata.getMessageId(), metadata.getState());
  }

  void delete(TaskMetadata metadata) throws PulsarClientException {
    send(metadata, true);
    log.debug("Deleted metadata for {}", metadata.getMessageId());
  }

  private void send(TaskMetadata metadata, boolean tombstone) throws PulsarClientException {
    producer.newMessage().key(metadata.getMessageId()).value(tombstone ? null : metadata).send();
  }
}
