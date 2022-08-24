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
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamnative.pulsar.recipes.task.ProcessExecutor.KeepAlive;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessExecutorTest {
  private static final Optional<Duration> NoMaxDuration = Optional.empty();
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  @Mock private Process<String, String> process;
  @Mock private KeepAlive keepAlive;
  @Mock private Clock clock;

  @Test
  void simpleSuccess() throws Exception {
    ProcessExecutor<String, String> processExecutor =
        new ProcessExecutor<>(executor, process, clock, Duration.ofMillis(100L));

    when(process.apply(TASK)).thenReturn(RESULT);

    String result = processExecutor.execute(TASK, NoMaxDuration, keepAlive);

    assertThat(result).isEqualTo(RESULT);
    verify(keepAlive, never()).update();
  }

  @Test
  void processorThrowsException() throws Exception {
    ProcessExecutor<String, String> processExecutor =
        new ProcessExecutor<>(executor, process, clock, Duration.ofMillis(100L));

    when(process.apply(TASK)).thenThrow(new Exception("failed"));

    assertThatExceptionOfType(ProcessException.class)
        .isThrownBy(() -> processExecutor.execute(TASK, NoMaxDuration, keepAlive))
        .withCauseExactlyInstanceOf(Exception.class)
        .havingCause()
        .withMessage("failed");
    verify(keepAlive, never()).update();
  }

  @Test
  void processorHasDelay() throws Exception {
    ProcessExecutor<String, String> processExecutor =
        new ProcessExecutor<>(executor, process, clock, Duration.ofMillis(75L));

    when(process.apply(TASK))
        .thenAnswer(
            __ -> {
              Thread.sleep(100L);
              return RESULT;
            });

    String result = processExecutor.execute(TASK, NoMaxDuration, keepAlive);

    assertThat(result).isEqualTo(RESULT);
    verify(keepAlive, times(1)).update();
  }

  @Test
  void processorInterrupted() throws Exception {
    ProcessExecutor<String, String> processExecutor =
        new ProcessExecutor<>(executor, process, clock, Duration.ofMillis(100L));

    when(process.apply(TASK))
        .thenAnswer(
            __ -> {
              Thread.currentThread().interrupt();
              Thread.sleep(10_000L);
              return RESULT;
            });

    assertThatExceptionOfType(ProcessException.class)
        .isThrownBy(() -> processExecutor.execute(TASK, NoMaxDuration, keepAlive))
        .withCauseExactlyInstanceOf(InterruptedException.class);
    verify(keepAlive, never()).update();
  }

  @Test
  void processTaskWithinDuration() throws Exception {
    Optional<Duration> oneHour = Optional.of(Duration.ofHours(1L));
    when(clock.instant())
        .thenReturn(Instant.ofEpochMilli(0), Instant.ofEpochMilli(MINUTES.toMillis(5)));
    ProcessExecutor<String, String> processExecutor =
        new ProcessExecutor<>(executor, process, clock, Duration.ofMillis(50L));

    when(process.apply(TASK))
        .thenAnswer(
            __ -> {
              Thread.sleep(100L);
              return RESULT;
            });

    String result = processExecutor.execute(TASK, oneHour, keepAlive);
    assertThat(result).isEqualTo(RESULT);
  }

  @Test
  @Timeout(value = 10, unit = SECONDS)
  void processTaskExceedsDuration() throws Exception {
    Optional<Duration> oneHour = Optional.of(Duration.ofHours(1L));
    when(clock.instant())
        .thenReturn(Instant.ofEpochMilli(0), Instant.ofEpochMilli(MINUTES.toMillis(65)));
    ProcessExecutor<String, String> processExecutor =
        new ProcessExecutor<>(executor, process, clock, Duration.ofMillis(100L));

    when(process.apply(TASK))
        .thenAnswer(
            __ -> {
              // We expect this sleep to be interrupted
              Thread.sleep(MINUTES.toMillis(120));
              return RESULT;
            });

    assertThatExceptionOfType(ProcessException.class)
        .isThrownBy(() -> processExecutor.execute(TASK, oneHour, keepAlive))
        .withMessage("Task exceeded maximum execution duration - terminated.")
        .withCauseExactlyInstanceOf(CancellationException.class);
    verify(keepAlive, times(1)).update();
  }
}
