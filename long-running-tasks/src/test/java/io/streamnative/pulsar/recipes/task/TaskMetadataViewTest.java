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

import static io.streamnative.pulsar.recipes.task.TaskState.NEW;
import static io.streamnative.pulsar.recipes.task.TaskState.PROCESSING;
import static io.streamnative.pulsar.recipes.task.TestUtils.ENCODED_TASK;
import static io.streamnative.pulsar.recipes.task.TestUtils.MESSAGE_ID;
import static io.streamnative.pulsar.recipes.task.TestUtils.TASK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Clock;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskMetadataViewTest {
  @Mock private TableView<TaskMetadata> tableView;
  @Mock private Clock clock;
  @Mock private Message<String> message;
  @Mock private MessageId messageId;
  private TaskMetadataView<String> metadataView;
  private final Schema<String> taskSchema = Schema.STRING;

  @BeforeEach
  void beforeEach() {
    when(message.getMessageId()).thenReturn(messageId);
    when(messageId.toString()).thenReturn(MESSAGE_ID);
    metadataView = new TaskMetadataView<>(tableView, clock, taskSchema);
  }

  @Test
  void unseenMessageId() {
    when(message.getValue()).thenReturn(TASK);
    when(clock.millis()).thenReturn(0L);

    when(tableView.get(MESSAGE_ID)).thenReturn(null);

    assertThat(metadataView.get(message))
        .isEqualTo(new TaskMetadata(MESSAGE_ID, NEW, 0L, 0L, 0, ENCODED_TASK, null, null));
  }

  @Test
  void previouslySeenMessageId() {
    var taskMetadata =
        new TaskMetadata(MESSAGE_ID, PROCESSING, 0L, 10L, 1, ENCODED_TASK, null, null);
    when(tableView.get(MESSAGE_ID)).thenReturn(taskMetadata);

    assertThat(metadataView.get(message)).isEqualTo(taskMetadata);
  }
}
