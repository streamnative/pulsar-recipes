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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class ProcessExecutor<T, R> {
  private final ScheduledExecutorService executor;
  private final Process<T, R> process;
  private final Clock clock;
  private final long keepAliveIntervalMillis;

  R execute(T task, Optional<Duration> maxTaskDuration, KeepAlive keepAlive)
      throws ProcessException {
    Instant start = clock.instant();
    ScheduledFuture<R> future = executor.schedule(() -> this.process.apply(task), 0L, MILLISECONDS);
    try {
      while (!future.isDone()) {
        try {
          future.get(keepAliveIntervalMillis, MILLISECONDS);
        } catch (TimeoutException e) {
          try {
            keepAlive.update();
          } catch (Exception e2) {
            log.warn("Failed to update keep alive", e2);
          }
          if (maxTaskDuration.isPresent()
              && clock.instant().isAfter(start.plus(maxTaskDuration.get()))) {
            future.cancel(true);
            throw new ProcessException("Task exceeded maximum execution duration - terminated.");
          }
        }
      }
      return future.get();
    } catch (ProcessException e) {
      throw e;
    } catch (ExecutionException e) {
      throw new ProcessException(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ProcessException(e);
    } catch (Exception e) {
      throw new ProcessException(e);
    }
  }

  interface KeepAlive {
    void update() throws Exception;
  }
}
