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

import static io.streamnative.pulsar.recipes.task.TaskState.COMPLETED;
import static io.streamnative.pulsar.recipes.task.TaskState.FAILED;
import static io.streamnative.pulsar.recipes.task.TaskState.NEW;
import static io.streamnative.pulsar.recipes.task.TaskState.PROCESSING;
import static io.streamnative.pulsar.recipes.task.TestUtils.ENCODED_RESULT;
import static io.streamnative.pulsar.recipes.task.TestUtils.ENCODED_TASK;
import static io.streamnative.pulsar.recipes.task.TestUtils.FAILURE_REASON;
import static io.streamnative.pulsar.recipes.task.TestUtils.MESSAGE_ID;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

class TaskProcessingStateTest {
  private final long now = 0L;
  private final long newNow = 1L;
  private final TaskProcessingState newProcessingState =
      new TaskProcessingState(MESSAGE_ID, NEW, now, now, 0, ENCODED_TASK, null, null);

  @Test
  void ofShouldCreateNewState() {
    assertThat(TaskProcessingState.of(MESSAGE_ID, now, ENCODED_TASK)).isEqualTo(newProcessingState);
  }

  @Test
  void processShouldTransitionToProcessing() {
    assertThat(newProcessingState.process(newNow))
        .isEqualTo(
            new TaskProcessingState(
                MESSAGE_ID, PROCESSING, now, newNow, 1, ENCODED_TASK, null, null));
  }

  @Test
  void keepALiveShouldUpdateLastUpdated() {
    assertThat(newProcessingState.keepAlive(newNow))
        .isEqualTo(
            new TaskProcessingState(MESSAGE_ID, NEW, now, newNow, 0, ENCODED_TASK, null, null));
  }

  @Test
  void completeShouldTransitionToCompleted() {
    assertThat(newProcessingState.complete(newNow, ENCODED_RESULT))
        .isEqualTo(
            new TaskProcessingState(
                MESSAGE_ID, COMPLETED, now, newNow, 0, ENCODED_TASK, ENCODED_RESULT, null));
  }

  @Test
  void failShouldTransitionToFailed() {
    assertThat(newProcessingState.fail(newNow, FAILURE_REASON))
        .isEqualTo(
            new TaskProcessingState(
                MESSAGE_ID, FAILED, now, newNow, 0, ENCODED_TASK, null, FAILURE_REASON));
  }

  @Test
  void serde() {
    TaskProcessingState taskProcessingState =
        new TaskProcessingState(
            MESSAGE_ID, COMPLETED, now, newNow, 0, ENCODED_TASK, ENCODED_RESULT, FAILURE_REASON);
    Schema<TaskProcessingState> schema = Schema.JSON(TaskProcessingState.class);
    byte[] bytes = schema.encode(taskProcessingState);
    TaskProcessingState result = schema.decode(bytes);
    assertThat(result).isEqualTo(taskProcessingState);
  }
}
