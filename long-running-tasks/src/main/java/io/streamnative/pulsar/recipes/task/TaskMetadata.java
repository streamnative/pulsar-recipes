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

import lombok.ToString;
import lombok.Value;

/**
 * Describes the last known state of a task undergoing {@link
 * io.streamnative.pulsar.recipes.task.Process processing}.
 */
@Value
@ToString(exclude = {"task", "result"}) // task and result may be sensitive
public class TaskMetadata {
  String messageId;
  TaskState state;
  long created;
  long lastUpdated;
  int attempts;
  byte[] task;
  byte[] result;
  String failureReason;

  public static TaskMetadata of(String messageId, long now, byte[] task) {
    return new TaskMetadata(messageId, NEW, now, now, 0, task, null, null);
  }

  private TaskMetadata transition(
      TaskState state, long lastUpdated, int attempts, byte[] result, String failureReason) {
    return new TaskMetadata(
        messageId, state, created, lastUpdated, attempts, task, result, failureReason);
  }

  public TaskMetadata process(long now) {
    return transition(PROCESSING, now, attempts + 1, result, null);
  }

  public TaskMetadata keepAlive(long now) {
    return transition(state, now, attempts, result, failureReason);
  }

  public TaskMetadata complete(long now, byte[] result) {
    return transition(COMPLETED, now, attempts, result, failureReason);
  }

  public TaskMetadata fail(long now, String failureReason) {
    return transition(FAILED, now, attempts, result, failureReason);
  }
}
