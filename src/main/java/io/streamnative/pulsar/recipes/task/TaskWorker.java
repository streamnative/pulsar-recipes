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
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static lombok.AccessLevel.PACKAGE;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
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

  public static <T, R> TaskWorker create(
      PulsarClient client, Process<T, R> process, TaskWorkerConfiguration<T, R> configuration)
      throws PulsarClientException {
    ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    Clock clock = systemUTC();

    Schema<TaskMetadata> stateSchema = Schema.JSON(TaskMetadata.class);

    MessagingFactory<T> factory = new MessagingFactory<>(client, stateSchema, configuration);
    List<AutoCloseable> closeables = new ArrayList<>();

    Producer<TaskMetadata> metadataProducer = factory.taskMetadataProducer();
    closeables.add(metadataProducer);
    TaskMetadataUpdater stateUpdater = new TaskMetadataUpdater(metadataProducer);

    TableView<TaskMetadata> metadataTableView = factory.taskMetadataTableView();
    closeables.add(metadataTableView);
    TaskMetadataView<T> taskMetadataView =
        new TaskMetadataView<>(metadataTableView, clock, configuration.getTaskSchema());

    ProcessExecutor<T, R> processExecutor =
        new ProcessExecutor<>(
            executor, process, clock, configuration.getKeepAliveInterval().toMillis());
    TaskListener<T, R> taskListener =
        new TaskListener<>(
            taskMetadataView,
            stateUpdater,
            processExecutor,
            clock,
            configuration.getResultSchema(),
            configuration.getMaxTaskAttempts(),
            configuration.getKeepAliveInterval().toMillis());
    Consumer<T> taskConsumer = factory.taskConsumer(taskListener);
    closeables.add(taskConsumer);

    TaskMetadataEvictionListener evictionListener =
        new TaskMetadataEvictionListener(
            stateUpdater,
            clock,
            configuration.getMaxTaskAttempts(),
            configuration.getRetention().toMillis());
    Consumer<TaskMetadata> metadataConsumer = factory.taskMetadataConsumer(evictionListener);
    closeables.add(metadataConsumer);

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
