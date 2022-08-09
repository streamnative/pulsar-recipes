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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class TaskHandler<T, R> {
  private final Executor executor;
  private final TaskProcessor<T, R> taskProcessor;
  private final long keepAliveIntervalMillis;

  R handleTask(T task, KeepAlive keepAlive) throws TaskException {
    CompletableFuture<R> future = new CompletableFuture<>();
    executor.execute(
        () -> {
          try {
            future.complete(taskProcessor.process(task));
          } catch (Exception e) {
            future.completeExceptionally(e);
          }
        });

    try {
      // TODO have maximum runtime to force failure?
      while (!future.isDone()) {
        try {
          future.get(keepAliveIntervalMillis, MILLISECONDS);
        } catch (TimeoutException e) {
          try {
            keepAlive.update();
          } catch (Exception e2) {
            log.warn("Failed to update keep alive", e2);
          }
        }
      }
      return future.get();
    } catch (ExecutionException e) {
      throw new TaskException(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TaskException(e);
    } catch (Exception e) {
      throw new TaskException(e);
    }
  }

  interface KeepAlive {
    void update() throws Exception;
  }
}
