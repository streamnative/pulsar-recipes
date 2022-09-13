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

import static java.time.Duration.ZERO;
import static java.util.Objects.requireNonNull;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Schema;

/**
 * Configures the worker component of the scheduler system.
 *
 * @param <T> Type describing the task to be processed.
 * @param <R> Return type yielded from the processing of the task.
 */
@Getter
@AllArgsConstructor(access = PRIVATE)
public class TaskWorkerConfiguration<T, R> {
  private final Schema<T> taskSchema;
  private final Schema<R> resultSchema;
  private final Schema<TaskMetadata> metadataSchema;
  private final String taskTopic;
  private final String metadataTopic;
  private final String subscription;
  private final int maxTaskAttempts;
  private final Duration keepAliveInterval;
  private final Duration workerTaskTimeout;
  private final Duration taskRedeliveryDelay;
  private final Duration retention;
  private final Duration shutdownTimeout;

  public static <T, R> Builder<T, R> builder(Schema<T> taskSchema, Schema<R> resultSchema) {
    return new Builder<>(taskSchema, resultSchema);
  }

  @RequiredArgsConstructor(access = PRIVATE)
  public static class Builder<T, R> {
    private final Schema<T> taskSchema;
    private final Schema<R> resultSchema;
    private Schema<TaskMetadata> metadataSchema = Schema.JSON(TaskMetadata.class);
    private String taskTopic;
    private String stateTopic;
    private String subscription;
    private int maxTaskAttempts = 3;
    private Duration keepAliveInterval = Duration.ofMinutes(5);
    private Duration workerTaskTimeout = ZERO;
    private Duration taskRedeliveryDelay = Duration.ofMinutes(5);
    private Duration retention = Duration.ofDays(1);
    private Duration shutdownTimeout = Duration.ofSeconds(10);

    public Builder<T, R> metadataSchema(@NonNull Schema<TaskMetadata> metadataSchema) {
      this.metadataSchema = metadataSchema;
      return this;
    }

    /**
     * The topic that the worker will listen for tasks on.
     *
     * @param taskTopic The task topic
     * @return this TaskWorkerBuilder instance
     */
    public Builder<T, R> taskTopic(@NonNull String taskTopic) {
      checkArgument(isNotBlank(taskTopic), "taskTopic must not be blank");
      this.taskTopic = taskTopic;
      return this;
    }

    /**
     * The topic that the worker will use for managing task processing state.
     *
     * @param stateTopic The state topic
     * @return this TaskWorkerBuilder instance
     */
    public Builder<T, R> stateTopic(@NonNull String stateTopic) {
      checkArgument(isNotBlank(stateTopic), "stateTopic must not be blank");
      this.stateTopic = stateTopic;
      return this;
    }

    /**
     * The subscription name that will be used for listening for tasks and receiving state updates.
     *
     * @param subscription The subscription name
     * @return this TaskWorkerBuilder instance
     */
    public Builder<T, R> subscription(@NonNull String subscription) {
      checkArgument(isNotBlank(subscription), "subscription must not be blank");
      this.subscription = subscription;
      return this;
    }

    /**
     * The maximum number of times to attempt to process a task.
     *
     * @param maxTaskAttempts The maximum number of attempts
     * @return this TaskWorkerBuilder instance
     */
    public Builder<T, R> maxTaskAttempts(int maxTaskAttempts) {
      checkArgument(maxTaskAttempts > 0, " maxTaskAttempts must be greater than zero");
      this.maxTaskAttempts = maxTaskAttempts;
      return this;
    }

    /**
     * The delay at which tasks should be redelivered after an unexpected error in the handling on
     * the task or the task metadata. These failures are exceptional and unlikely to be related to
     * the processing of the task.
     *
     * @param taskRedeliveryDelay The task redelivery delay
     * @return this TaskWorkerBuilder instance
     */
    public Builder<T, R> taskRedeliveryDelay(@NonNull Duration taskRedeliveryDelay) {
      checkArgument(
          taskRedeliveryDelay.compareTo(ZERO) > 0, "taskRedeliveryDelay must be greater than zero");
      this.taskRedeliveryDelay = taskRedeliveryDelay;
      return this;
    }

    /**
     * The interval at which an executing task should be checked for completion and the processing
     * state updated.
     *
     * @param keepAliveInterval The keep alive interval
     * @return this TaskWorkerBuilder instance
     */
    public Builder<T, R> keepAliveInterval(@NonNull Duration keepAliveInterval) {
      checkArgument(
          keepAliveInterval.compareTo(ZERO) > 0, "keepAliveInterval must be greater than zero");
      this.keepAliveInterval = keepAliveInterval;
      return this;
    }

    /**
     * The maximum time before a task is potentially released to other workers. If the original
     * consumer is still working on the task this may result in duplicate work. Primarily this is to
     * defend against live but stuck workers. If avoiding duplicate work is a priority, disable this
     * feature with a value of {@link Duration#ZERO} (the default). If your priority is to complete
     * all work, set this to a value that exceeds your longest expected job run (including retries).
     *
     * @param workerTaskTimeout Max time allotted to workers to complete a task.
     * @return this TaskWorkerBuilder instance
     */
    public Builder<T, R> workerTaskTimeout(@NonNull Duration workerTaskTimeout) {
      checkArgument(
          workerTaskTimeout.compareTo(ZERO) >= 0, "workerTaskTimeout must be zero or greater");
      this.workerTaskTimeout = workerTaskTimeout;
      return this;
    }

    /**
     * The retention of task processing state once a task has reached a terminal state (either
     * completed or failed with maximum attempts).
     *
     * @param retention The retention
     * @return this TaskWorkerBuilder instance
     */
    public Builder<T, R> retention(@NonNull Duration retention) {
      checkArgument(retention.compareTo(ZERO) > 0, "retention must be greater than zero");
      this.retention = retention;
      return this;
    }

    /**
     * How long to wait for the currently running task to complete before forcibly terminating it on
     * shutdown.
     *
     * @param shutdownTimeout The shutdown timeout
     * @return this TaskWorkerBuilder instance
     */
    public Builder<T, R> shutdownTimeout(@NonNull Duration shutdownTimeout) {
      checkArgument(
          shutdownTimeout.compareTo(ZERO) > 0, "shutdownTimeout must be greater than zero");
      this.shutdownTimeout = shutdownTimeout;
      return this;
    }

    private void checkArgument(boolean expression, String errorMessage) {
      if (!expression) {
        throw new IllegalArgumentException(errorMessage);
      }
    }

    public TaskWorkerConfiguration<T, R> build() {
      requireNonNull(taskTopic);
      requireNonNull(subscription);

      var stateTopic = this.stateTopic == null ? taskTopic + "-state" : this.stateTopic;

      return new TaskWorkerConfiguration<>(
          taskSchema,
          resultSchema,
          metadataSchema,
          taskTopic,
          stateTopic,
          subscription,
          maxTaskAttempts,
          keepAliveInterval,
          workerTaskTimeout,
          taskRedeliveryDelay,
          retention,
          shutdownTimeout);
    }
  }
}
