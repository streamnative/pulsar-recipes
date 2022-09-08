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
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static lombok.AccessLevel.PACKAGE;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

/** Responsible for the processing of tasks and management of resulting task metadata. */
@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
public class TaskWorker implements AutoCloseable {
  private final ExecutorService executor;
  private final List<AutoCloseable> closeables;
  private final long shutdownTimeoutMillis;

  public static <T, R> TaskWorker create(
      PulsarClient client, Process<T, R> process, TaskWorkerConfiguration<T, R> configuration)
      throws PulsarClientException {
    var executor = newSingleThreadExecutor();
    var clock = systemUTC();

    var stateSchema = Schema.JSON(TaskMetadata.class);

    var factory = new MessagingFactory<>(client, stateSchema, configuration);
    var closeables = new ArrayList<AutoCloseable>();

    var metadataProducer = factory.taskMetadataProducer();
    closeables.add(metadataProducer);
    var stateUpdater = new TaskMetadataUpdater(metadataProducer);

    var metadataTableView = factory.taskMetadataTableView();
    closeables.add(metadataTableView);
    var taskMetadataView =
        new TaskMetadataView<>(metadataTableView, clock, configuration.getTaskSchema());

    var processExecutor =
        new ProcessExecutor<>(executor, process, clock, configuration.getKeepAliveInterval());
    var taskListener =
        new TaskListener<>(
            taskMetadataView,
            stateUpdater,
            processExecutor,
            clock,
            configuration.getResultSchema(),
            configuration.getMaxTaskAttempts(),
            configuration.getKeepAliveInterval().toMillis());
    var taskConsumer = factory.taskConsumer(taskListener);
    closeables.add(taskConsumer);

    var evictionListener =
        new TaskMetadataEvictionListener(
            stateUpdater,
            clock,
            configuration.getMaxTaskAttempts(),
            configuration.getRetention().toMillis());
    var metadataConsumer = factory.metadataEvictionConsumer(evictionListener);
    closeables.add(metadataConsumer);

    return new TaskWorker(
        executor, unmodifiableList(closeables), configuration.getShutdownTimeout().toMillis());
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
