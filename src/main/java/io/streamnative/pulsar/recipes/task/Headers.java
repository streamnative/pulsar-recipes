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


import java.util.Optional;
import lombok.NonNull;
import org.apache.pulsar.client.api.Message;

public enum Headers {
  /**
   * Sets the maximum length of time a task may execute for - declared as an <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO 8601 Duration</a> such as {@code
   * P3D5H}.
   */
  MAX_TASK_DURATION() {
    String key() {
      return "MAX_TASK_DURATION";
    }
  };

  abstract String key();

  Optional<String> from(@NonNull Message<?> message) {
    return Optional.ofNullable(message.getProperty(key()));
  }
}
