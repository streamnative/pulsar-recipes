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

import static io.streamnative.pulsar.recipes.task.TestUtils.RESULT;
import static io.streamnative.pulsar.recipes.task.TestUtils.TASK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamnative.pulsar.recipes.task.TaskHandler.KeepAlive;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskHandlerTest {
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  @Mock private TaskProcessor<String, String> taskProcessor;
  @Mock private KeepAlive keepAlive;

  @Test
  void simpleSuccess() throws Exception {
    TaskHandler<String, String> taskHandler = new TaskHandler<>(executor, taskProcessor, 100L);

    when(taskProcessor.process(TASK)).thenReturn(RESULT);

    String result = taskHandler.handleTask(TASK, keepAlive);

    assertThat(result).isEqualTo(RESULT);
    verify(keepAlive, never()).update();
  }

  @Test
  void processorThrowsException() throws Exception {
    TaskHandler<String, String> taskHandler = new TaskHandler<>(executor, taskProcessor, 100L);

    when(taskProcessor.process(TASK)).thenThrow(new Exception("failed"));

    assertThatExceptionOfType(TaskException.class)
        .isThrownBy(() -> taskHandler.handleTask(TASK, keepAlive))
        .withCauseExactlyInstanceOf(Exception.class)
        .havingCause()
        .withMessage("failed");
    verify(keepAlive, never()).update();
  }

  @Test
  void processorHasDelay() throws Exception {
    TaskHandler<String, String> taskHandler = new TaskHandler<>(executor, taskProcessor, 75L);

    when(taskProcessor.process(TASK))
        .thenAnswer(
            __ -> {
              Thread.sleep(100L);
              return RESULT;
            });

    String result = taskHandler.handleTask(TASK, keepAlive);

    assertThat(result).isEqualTo(RESULT);
    verify(keepAlive, times(1)).update();
  }

  @Test
  void processorInterrupted() throws Exception {
    TaskHandler<String, String> taskHandler = new TaskHandler<>(executor, taskProcessor, 100L);

    when(taskProcessor.process(TASK))
        .thenAnswer(
            i -> {
              Thread.currentThread().interrupt();
              Thread.sleep(10_000L);
              return RESULT;
            });

    assertThatExceptionOfType(TaskException.class)
        .isThrownBy(() -> taskHandler.handleTask(TASK, keepAlive))
        .withCauseExactlyInstanceOf(InterruptedException.class);
    verify(keepAlive, never()).update();
  }
}
