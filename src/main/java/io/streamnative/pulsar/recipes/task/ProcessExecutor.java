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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Applies a {@link Process} to a task and observes the process until it has either completed, or
 * (optionally) exceeded it's allowed processing duration — as defined by the message header {@link
 * TaskProperties#MAX_TASK_DURATION}.
 *
 * @param <T> Type describing the task to be processed.
 * @param <R> Return type yielded from the processing of the task.
 */
@Slf4j
@RequiredArgsConstructor
class ProcessExecutor<T, R> {
  private final ScheduledExecutorService executor;
  private final Process<T, R> process;
  private final Clock clock;
  private final Duration keepAliveInterval;

  R execute(T task, Optional<Duration> maxTaskDuration, KeepAlive keepAlive)
      throws ProcessException {
    Instant start = clock.instant();
    ScheduledFuture<R> future = executor.schedule(() -> this.process.apply(task), 0L, MILLISECONDS);
    try {
      while (!future.isDone()) {
        try {
          future.get(keepAliveInterval.toMillis(), MILLISECONDS);
        } catch (TimeoutException e) {
          if (maxTaskDuration.isPresent()
              && clock.instant().isAfter(start.plus(maxTaskDuration.get()))) {
            future.cancel(true);
          } else {
            try {
              keepAlive.update();
            } catch (Exception e2) {
              log.warn("Failed to update keep alive", e2);
            }
          }
        }
      }
      return future.get();
    } catch (CancellationException e) {
      throw new ProcessException("Process was cancelled", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ProcessException("Task worker thread interrupted", e);
    } catch (ExecutionException e) {
      throw new ProcessException("Processing error", e.getCause());
    } catch (Exception e) {
      throw new ProcessException("Unexpected error in executor", e);
    }
  }

  interface KeepAlive {
    void update() throws Exception;
  }
}
