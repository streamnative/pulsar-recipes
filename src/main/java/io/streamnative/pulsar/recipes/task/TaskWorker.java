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

import static java.time.Clock.systemUTC;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static lombok.AccessLevel.PACKAGE;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;

@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
public class TaskWorker implements AutoCloseable {
  private final ExecutorService executor;
  private final List<AutoCloseable> closeables;
  private final long shutdownTimeoutMillis;

  // TODO main method & CLI?

  public static <T, R> TaskWorker create(
      PulsarClient client,
      TaskProcessor<T, R> taskProcessor,
      TaskWorkerConfiguration<T, R> configuration)
      throws PulsarClientException {
    ExecutorService executor = newSingleThreadExecutor();
    Clock clock = systemUTC();

    Schema<TaskProcessingState> stateSchema = Schema.JSON(TaskProcessingState.class);

    MessagingFactory<T> factory = new MessagingFactory<>(client, stateSchema, configuration);
    List<AutoCloseable> closeables = new ArrayList<>();

    Producer<TaskProcessingState> taskStateProducer = factory.taskStateProducer();
    closeables.add(taskStateProducer);
    TaskStateUpdater stateUpdater = new TaskStateUpdater(taskStateProducer);

    TableView<TaskProcessingState> taskStateTableView = factory.taskStateTableView();
    closeables.add(taskStateTableView);
    TaskStateView<T> taskStateView =
        new TaskStateView<>(taskStateTableView, clock, configuration.getTaskSchema());

    TaskHandler<T, R> taskHandler =
        new TaskHandler<>(executor, taskProcessor, configuration.getKeepAliveInterval().toMillis());
    TaskListener<T, R> taskListener =
        new TaskListener<>(
            taskStateView,
            stateUpdater,
            taskHandler,
            clock,
            configuration.getResultSchema(),
            configuration.getMaxAttempts(),
            configuration.getKeepAliveInterval().toMillis());
    Consumer<T> taskConsumer = factory.taskConsumer(taskListener);
    closeables.add(taskConsumer);

    ExpirationListener expirationListener =
        new ExpirationListener(
            stateUpdater,
            clock,
            configuration.getMaxAttempts(),
            configuration.getRetention().toMillis());
    Consumer<TaskProcessingState> taskStateConsumer = factory.taskStateConsumer(expirationListener);
    closeables.add(taskStateConsumer);

    return new TaskWorker(executor, closeables, configuration.getShutdownTimeout().toMillis());
  }

  @Override
  public void close() {
    shutdownExecutor();
    closeables.forEach(this::closeQuietly);
  }

  private void shutdownExecutor() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(shutdownTimeoutMillis, MILLISECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }
  }

  private void closeQuietly(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception e) {
      log.error("Error closing {}", closeable, e);
    }
  }
}
