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
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;

/**
 * A global, eventually consistent {@link org.apache.pulsar.client.api.TableView table view} of
 * {@link TaskMetadata metadata} pertaining to all tasks being processed by the system.
 *
 * @param <T> Type describing the task to be processed.s
 */
@RequiredArgsConstructor
class TaskMetadataView<T> {
  private final TableView<TaskMetadata> tableView;
  private final Clock clock;
  private final Schema<T> taskSchema;

  TaskMetadata get(Message<T> message) {
    var messageId = message.getMessageId().toString();
    var taskMetadata = tableView.get(messageId);
    if (taskMetadata == null) {
      var encodedTask = taskSchema.encode(message.getValue());
      return TaskMetadata.of(messageId, clock.millis(), encodedTask);
    }
    return taskMetadata;
  }
}
