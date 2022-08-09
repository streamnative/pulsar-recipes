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
class StateView<T> {
  private final TableView<ProcessingState> tableView;
  private final Clock clock;
  private final Schema<T> taskSchema;

  ProcessingState get(Message<T> message) {
    String messageId = message.getMessageId().toString();
    ProcessingState processingState = tableView.get(messageId);
    if (processingState == null) {
      byte[] encodedTask = taskSchema.encode(message.getValue());
      return ProcessingState.of(messageId, clock.millis(), encodedTask);
    }
    return processingState;
  }
}
