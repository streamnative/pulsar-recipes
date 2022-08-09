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

import static io.streamnative.pulsar.recipes.task.State.COMPLETED;
import static io.streamnative.pulsar.recipes.task.State.FAILED;
import static io.streamnative.pulsar.recipes.task.State.NEW;
import static io.streamnative.pulsar.recipes.task.State.PROCESSING;

import lombok.ToString;
import lombok.Value;

@Value
@ToString(exclude = {"task", "result"}) // task and result may be sensitive
public class ProcessingState {
  String messageId;
  State state;
  long created;
  long lastUpdated;
  int attempts;
  byte[] task;
  byte[] result;
  String failureReason;

  public static ProcessingState of(String messageId, long now, byte[] task) {
    return new ProcessingState(messageId, NEW, now, now, 0, task, null, null);
  }

  private ProcessingState transition(
      State state, long lastUpdated, int attempts, byte[] result, String failureReason) {
    return new ProcessingState(
        messageId, state, created, lastUpdated, attempts, task, result, failureReason);
  }

  public ProcessingState process(long now) {
    return transition(PROCESSING, now, attempts + 1, result, null);
  }

  public ProcessingState keepAlive(long now) {
    return transition(state, now, attempts, result, failureReason);
  }

  public ProcessingState complete(long now, byte[] result) {
    return transition(COMPLETED, now, attempts, result, failureReason);
  }

  public ProcessingState fail(long now, String failureReason) {
    return transition(FAILED, now, attempts, result, failureReason);
  }
}
