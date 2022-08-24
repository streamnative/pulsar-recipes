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

import static io.streamnative.pulsar.recipes.task.Headers.MAX_TASK_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.time.DateTimeException;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HeadersTest {

  @Mock Message<String> message;

  @Test
  void maxTaskDurationKey() {
    assertThat(MAX_TASK_DURATION.key()).isEqualTo("MAX_TASK_DURATION");
  }

  @Test
  void ofValidDuration() {
    assertThatCode(() -> MAX_TASK_DURATION.of("P3DT1H2M")).doesNotThrowAnyException();
  }

  @Test
  void ofInvalidDuration() {
    assertThatThrownBy(() -> MAX_TASK_DURATION.of("P1H")).isInstanceOf(DateTimeException.class);
  }

  @Test
  void maxTaskDurationFrom() {
    when(message.getProperty(MAX_TASK_DURATION.key())).thenReturn("PT1H");
    assertThat(MAX_TASK_DURATION.from(message)).isEqualTo(Optional.of("PT1H"));
  }
}
