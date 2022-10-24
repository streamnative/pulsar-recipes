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

final class TestUtils {
  private TestUtils() {}

  static final String MESSAGE_ID = "messageId";
  static final String TASK = "task";
  static final byte[] ENCODED_TASK = TASK.getBytes();
  static final String RESULT = "result";
  static final byte[] ENCODED_RESULT = RESULT.getBytes();

  static final String FAILURE_REASON = "failureReason";

  static TaskMetadata newState() {
    return new TaskMetadata(MESSAGE_ID, NEW, 0, 0, 0, ENCODED_TASK, null, null);
  }

  static TaskMetadata processingState(int attempts) {
    return new TaskMetadata(MESSAGE_ID, PROCESSING, 0, 0, attempts, ENCODED_TASK, null, null);
  }

  static TaskMetadata completedState(int attempts) {
    return new TaskMetadata(
        MESSAGE_ID, COMPLETED, 0, 0, attempts, ENCODED_TASK, ENCODED_RESULT, null);
  }

  static TaskMetadata failedState(int attempts) {
    return new TaskMetadata(MESSAGE_ID, FAILED, 0, 0, attempts, ENCODED_TASK, null, null);
  }
}
