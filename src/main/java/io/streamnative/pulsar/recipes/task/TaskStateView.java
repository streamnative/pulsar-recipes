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

@RequiredArgsConstructor
class TaskStateView<T> {
  private final TableView<TaskProcessingState> tableView;
  private final Clock clock;
  private final Schema<T> taskSchema;

  TaskProcessingState get(Message<T> message) {
    String messageId = message.getMessageId().toString();
    TaskProcessingState taskProcessingState = tableView.get(messageId);
    if (taskProcessingState == null) {
      byte[] encodedTask = taskSchema.encode(message.getValue());
      return TaskProcessingState.of(messageId, clock.millis(), encodedTask);
    }
    return taskProcessingState;
  }
}
