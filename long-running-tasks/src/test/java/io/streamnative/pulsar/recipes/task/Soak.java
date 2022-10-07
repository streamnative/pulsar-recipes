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

import static io.streamnative.pulsar.recipes.task.SingletonPulsarContainer.pulsar;
import static io.streamnative.pulsar.recipes.task.Soak.Task.Outcome.EXCEPTION;
import static io.streamnative.pulsar.recipes.task.Soak.Task.Outcome.RESULT;
import static io.streamnative.pulsar.recipes.task.Soak.Task.Outcome.TIMEOUT;
import static io.streamnative.pulsar.recipes.task.TaskProperties.MAX_TASK_DURATION;
import static io.streamnative.pulsar.recipes.task.TaskState.COMPLETED;
import static io.streamnative.pulsar.recipes.task.TaskState.FAILED;
import static java.lang.Math.abs;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.commons.lang3.RandomUtils.nextDouble;
import static org.assertj.core.api.Assertions.assertThat;

import io.streamnative.pulsar.recipes.task.Soak.Task.Outcome;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.pulsar.shade.com.google.common.collect.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This is a soak test that is intended to be executed independently and with user-defined
 * parameters to simulate their expected workloads.
 */
@Disabled
@Slf4j
public class Soak {
  private final Clock clock = Clock.systemUTC();
  private final Schema<Task> taskSchema = Schema.JSON(Task.class);
  private final Schema<TaskMetadata> metadataSchema = Schema.JSON(TaskMetadata.class);
  private final Schema<Result> resultSchema = Schema.JSON(Result.class);

  private PulsarClient client;
  private Producer<Task> producer;
  private Consumer<TaskMetadata> consumer;

  private void createResources(String taskTopic) throws Exception {
    client = newPulsarClient(pulsar.getPulsarBrokerUrl());
    producer = client.newProducer(taskSchema).topic(taskTopic).enableBatching(false).create();
    consumer =
        client
            .newConsumer(metadataSchema)
            .topic(taskTopic + "-state")
            .subscriptionName(randomUUID().toString())
            .subscribe();
  }

  @AfterEach
  void afterEach() throws Exception {
    producer.close();
    consumer.close();
  }

  @Test
  @Timeout(value = 4, unit = HOURS)
  void soak() throws Exception {
    // Modify these parameters to match your expected workload.
    var soakTimeout = Duration.ofHours(4);
    var workerCount = 15;
    var keepAliveInterval = Duration.ofSeconds(1);
    var taskCount = 100;
    var meanTaskTime = Duration.ofMinutes(5);
    var maxRetries = 3;
    var taskDurationDistribution =
        new NormalDistribution(meanTaskTime.toSeconds(), meanTaskTime.toSeconds() / 2.0);
    var taskOutcomeDistribution =
        new EnumeratedDistribution<>(
            List.of(
                Pair.create(RESULT, 0.9),
                Pair.create(EXCEPTION, 0.75),
                Pair.create(TIMEOUT, 0.25)));

    // Begin test
    var taskTopic = randomUUID().toString();
    createResources(taskTopic);

    var configuration =
        TaskWorkerConfiguration.builder(taskSchema, resultSchema)
            .taskTopic(taskTopic)
            .subscription("subscription")
            .retention(Duration.ofSeconds(1))
            .keepAliveInterval(keepAliveInterval)
            .maxTaskAttempts(maxRetries)
            .build();

    // Start workers
    //
    var process = new DummyProcess(clock);
    var workers =
        IntStream.range(0, workerCount)
            .mapToObj(i -> newTaskWorker(process, pulsar.getPulsarBrokerUrl(), configuration))
            .collect(toUnmodifiableList());

    // Submit work
    //
    var tasks =
        new TaskFactory(maxRetries, taskDurationDistribution, taskOutcomeDistribution)
            .newTasks(taskCount);

    var unscheduledTasks =
        tasks.stream().map(this::send).filter(Optional::isPresent).collect(toUnmodifiableSet());
    var scheduledTasks =
        tasks.stream().filter(t -> !unscheduledTasks.contains(t)).collect(toUnmodifiableList());
    var cumulativeTaskTries = tasks.stream().mapToLong(t -> t.getTryOutcomes().size()).sum();
    scheduledTasks.forEach(t -> log.debug("Task: {}", t));

    log.info("Successfully scheduled {} tasks out of {}", scheduledTasks.size(), tasks.size());
    var counts =
        tasks.stream()
            .flatMap(t -> t.getTryOutcomes().stream())
            .collect(groupingBy(identity(), counting()));
    log.info(
        "Total expected task tries: {} (exception: {}, timeout: {}. success: {})",
        cumulativeTaskTries,
        counts.get(EXCEPTION),
        counts.get(TIMEOUT),
        counts.get(RESULT));

    // Collect results
    var received = 0L;
    var incompleteScenarios = 0L;
    var missedResults = 0L;
    var missedCancellations = 0L;
    var missedExceptions = 0L;
    var unexpectedStates = 0L;
    var unexpectedResults = 0L;

    Instant start = clock.instant();
    Instant soakHorizon = start.plus(soakTimeout);
    while (clock.instant().isBefore(soakHorizon) && received < cumulativeTaskTries) {
      var message = consumer.receive((int) meanTaskTime.toSeconds(), SECONDS);
      if (message != null) {
        var metadata = message.getValue();
        if (metadata != null && Set.of(COMPLETED, FAILED).contains(metadata.getState())) {
          received++;
          var task = taskSchema.decode(metadata.getTask());
          var result = Optional.ofNullable(metadata.getResult()).map(resultSchema::decode);
          var error = Optional.ofNullable(metadata.getFailureReason());
          if (task.getTryOutcomes().size() < metadata.getAttempts()) {
            incompleteScenarios++;
          } else {
            var outcome = task.getTryOutcomes().get(metadata.getAttempts() - 1);
            switch (outcome) {
              case RESULT:
                if (result.isEmpty()) {
                  missedResults++;
                }
                break;
              case TIMEOUT:
                if (error.isEmpty() || !error.get().startsWith("Process was cancelled")) {
                  missedCancellations++;
                }
                if (result.isPresent()) {
                  unexpectedResults++;
                }
                break;
              case EXCEPTION:
                if (error.isEmpty() || !error.get().startsWith("Processing error")) {
                  missedExceptions++;
                }
                if (result.isPresent()) {
                  unexpectedResults++;
                }
                break;
              default:
                unexpectedStates++;
                break;
            }
          }
          // Ideally, all but 'received' should be 0
          log.info(
              "Received: {}, Inconsistent: {}, MissedResult: {}, MissedCancellation: {}, MissedExcecption: {}, "
                  + "UnexpectedResult: {}, UnexpectedState: {}",
              received,
              incompleteScenarios,
              missedResults,
              missedCancellations,
              missedExceptions,
              unexpectedResults,
              unexpectedStates);
        }
      }
      consumer.acknowledge(message);
    }

    log.info("Soak test completed after {}", Duration.between(start, clock.instant()));

    // close workers
    workers.forEach(TaskWorker::close);

    assertThat(received).isGreaterThanOrEqualTo(cumulativeTaskTries);
    assertThat(incompleteScenarios).isEqualTo(0);
    assertThat(missedResults).isEqualTo(0);
    assertThat(missedCancellations).isEqualTo(0);
    assertThat(missedExceptions).isEqualTo(0);
    assertThat(unexpectedResults).isEqualTo(0);
    assertThat(unexpectedStates).isEqualTo(0);
  }

  @SneakyThrows
  private PulsarClient newPulsarClient(String url) {
    return PulsarClient.builder().serviceUrl(url).build();
  }

  @SneakyThrows
  private TaskWorker newTaskWorker(
      Process<Task, Result> process,
      String url,
      TaskWorkerConfiguration<Task, Result> configuration) {
    var client = newPulsarClient(url);
    return TaskWorker.create(client, process, configuration);
  }

  @ToString
  @EqualsAndHashCode
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  static class Task {
    static Task newTask(long id, Duration maxRuntime, List<Outcome> tryOutcomes) {
      return new Task(maxRuntime.toMillis(), id, tryOutcomes);
    }

    @Getter private final long maxRuntimeMs;
    @Getter private final long id;
    @Getter private final List<Outcome> tryOutcomes;

    @JsonIgnore
    public Duration getMaxRuntime() {
      return Duration.ofMillis(maxRuntimeMs);
    }

    enum Outcome {
      RESULT,
      EXCEPTION,
      TIMEOUT
    }
  }

  @Value
  static class Result {
    long elapsedTimeMs;
    int iteration;
  }

  @RequiredArgsConstructor
  static class DummyProcess implements Process<Task, Result> {
    private final Clock clock;
    @Getter private final Map<Task, Integer> retryTracker = new HashMap<>();

    @Override
    public Result apply(@NonNull Task task) throws Exception {
      int iteration = Optional.ofNullable(retryTracker.get(task)).map(i -> ++i).orElse(0);
      retryTracker.put(task, iteration);
      var start = clock.instant();
      var maxRuntimeMs = task.getMaxRuntime().toMillis();
      var desiredOutcome = task.getTryOutcomes().get(iteration);
      switch (desiredOutcome) {
        case RESULT:
          Thread.sleep((long) (nextDouble(0.0, 0.9) * maxRuntimeMs));
          return new Result(Duration.between(start, clock.instant()).toMillis(), iteration);
        case EXCEPTION:
          Thread.sleep((long) (nextDouble(0.0, 0.9) * maxRuntimeMs));
          throw new RuntimeException("Requested process failure");
        case TIMEOUT:
          Thread.sleep((long) (nextDouble(1.1, 1.5) * maxRuntimeMs));
          // Ideally we will not reach the next line
          throw new MissedCancellationException(task, iteration);
        default:
          throw new IllegalStateException("Unsupported outcome" + desiredOutcome.name());
      }
    }
  }

  @RequiredArgsConstructor
  static class TaskFactory {
    private final int maxRetries;
    private final RealDistribution durations;
    private final EnumeratedDistribution<Outcome> outcomes;

    List<Task> newTasks(int count) {
      return IntStream.range(0, count).mapToObj(this::newTask).collect(toUnmodifiableList());
    }

    Task newTask(int id) {
      var failures =
          IntStream.range(0, maxRetries)
              .mapToObj(i -> outcomes.sample())
              .takeWhile(o -> o != RESULT)
              .collect(toUnmodifiableList());
      // If we are not out of tries, complete with a result
      var tryOutcomes =
          Streams.concat(
                  failures.stream(),
                  failures.size() < maxRetries ? Stream.of(RESULT) : Stream.empty())
              .collect(toUnmodifiableList());
      var maxRuntime = Duration.ofSeconds(abs((long) durations.sample()));
      return Task.newTask(id, maxRuntime, tryOutcomes);
    }
  }

  private Optional<Task> send(Task task) {
    try {
      producer
          .newMessage()
          .property(MAX_TASK_DURATION.key(), task.getMaxRuntime().toString())
          .value(task)
          .send();
      return Optional.empty();
    } catch (Exception e) {
      return Optional.of(task);
    }
  }

  static class TaskSubmissionException extends RuntimeException {
    @Getter private final Task task;

    TaskSubmissionException(Task task, Throwable cause) {
      super(cause);
      this.task = task;
    }
  }

  static class MissedCancellationException extends RuntimeException {
    @Getter private final Task task;
    @Getter private final int iteration;

    MissedCancellationException(Task task, int iteration) {
      super("Unexpected cancellation failure");
      this.task = task;
      this.iteration = iteration;
    }
  }
}
