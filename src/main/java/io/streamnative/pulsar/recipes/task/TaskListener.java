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


import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

/**
 * Handles a task's lifecycle. Tasks are ACKed when the processing on them has completed, or when
 * we've exceeded the max number of task retires, each ending in failure. Tasks are NACKed when
 * there is an unexpected error. You should expect task redeliveries only in the event of consumer
 * failure, one of the explicit NACK conditions described earlier, or if the {@code
 * TaskWorkerConfiguration#getWorkerTaskTimeout()} has been exceeded.
 *
 * @param <T> Type describing the task to be processed.
 * @param <R> Return type yielded from the processing of the task.
 */
@Slf4j
@RequiredArgsConstructor
class TaskListener<T, R> implements MessageListener<T> {
  private final TaskMetadataView<T> taskMetadataView;
  private final TaskMetadataUpdater taskMetadataUpdater;
  private final ProcessExecutor<T, R> processExecutor;
  private final Clock clock;
  private final Schema<R> resultSchema;
  private final int maxTaskAttempts;
  private final long keepAliveIntervalMillis;

  /**
   * Invoked when there is a new task, or when a task is redelivered due to one of: consumer
   * failure, unexpected error, premature keep-alive update.
   */
  @Override
  public void received(Consumer<T> taskConsumer, Message<T> taskMessage) {
    log.debug("Received: {}", taskMessage.getMessageId());
    var metadata = taskMetadataView.get(taskMessage);
    try {
      switch (metadata.getState()) {
        case NEW:
          handleNew(taskConsumer, taskMessage, metadata);
          break;
        case PROCESSING:
          handleProcessing(taskConsumer, taskMessage, metadata);
          break;
        case COMPLETED:
          handleCompleted(taskConsumer, taskMessage);
          break;
        case FAILED:
          handleFailed(taskConsumer, taskMessage, metadata);
          break;
        default:
          log.error("Unexpected state: {}", metadata);
          handleError(
              taskConsumer, taskMessage, metadata, "Unexpected state: " + metadata.getState());
          break;
      }
    } catch (Throwable t) {
      log.error(
          "Unexpected error when consuming task: task={}, metadata={}", taskMessage, metadata, t);
      taskConsumer.negativeAcknowledge(taskMessage);
    }
  }

  private void handleNew(
      Consumer<T> taskConsumer, Message<T> taskMessage, TaskMetadata taskMetadata)
      throws PulsarClientException {
    processTask(taskConsumer, taskMessage, taskMetadata);
  }

  private void processTask(
      Consumer<T> taskConsumer, Message<T> taskMessage, TaskMetadata taskMetadata)
      throws PulsarClientException {
    var updatedMetadata = taskMetadata.process(clock.millis());
    taskMetadataUpdater.update(updatedMetadata);
    var keepAliveState = updatedMetadata;
    try {
      log.debug("Task processing for message {}", taskMessage.getMessageId());

      var result =
          processExecutor.execute(
              taskMessage.getValue(),
              getMaxTaskDuration(taskMessage),
              () -> taskMetadataUpdater.update(keepAliveState.keepAlive(clock.millis())));
      log.debug("Task processed for message {}", taskMessage.getMessageId());
      var encodedResult = resultSchema.encode(result);
      updatedMetadata = updatedMetadata.complete(clock.millis(), encodedResult);
      taskMetadataUpdater.update(updatedMetadata);
      taskConsumer.acknowledge(taskMessage);
    } catch (ProcessException e) {
      log.error("Error while processing task: {}", updatedMetadata, e);
      handleError(
          taskConsumer,
          taskMessage,
          updatedMetadata,
          e.getMessage() + ": " + e.getCause().getMessage());
    } catch (Exception e) {
      log.error("Error handling task result: {}", updatedMetadata, e);
    }
  }

  private void handleProcessing(
      Consumer<T> taskConsumer, Message<T> taskMessage, TaskMetadata taskMetadata)
      throws PulsarClientException {
    var millisSinceLastUpdate = clock.millis() - taskMetadata.getLastUpdated();
    if (millisSinceLastUpdate > keepAliveIntervalMillis * 2) {
      if (taskMetadata.getAttempts() < maxTaskAttempts) {
        // We've missed two keep-alives so we'll assume this task is dead and try again
        processTask(taskConsumer, taskMessage, taskMetadata);
      } else {
        taskMetadataUpdater.update(
            taskMetadata.fail(clock.millis(), "All attempts to process task failed."));
        taskConsumer.acknowledge(taskMessage);
      }
    }
  }

  private void handleCompleted(Consumer<T> taskConsumer, Message<T> taskMessage)
      throws PulsarClientException {
    taskConsumer.acknowledge(taskMessage);
  }

  private void handleFailed(
      Consumer<T> taskConsumer, Message<T> taskMessage, TaskMetadata taskMetadata)
      throws PulsarClientException {
    if (taskMetadata.getAttempts() < maxTaskAttempts) {
      processTask(taskConsumer, taskMessage, taskMetadata);
    } else {
      taskConsumer.acknowledge(taskMessage);
    }
  }

  private void handleError(
      Consumer<T> taskConsumer,
      Message<T> taskMessage,
      TaskMetadata taskMetadata,
      String failureReason)
      throws PulsarClientException {
    var failedTaskMetadata = taskMetadata.fail(clock.millis(), failureReason);
    taskMetadataUpdater.update(failedTaskMetadata);
    handleFailed(taskConsumer, taskMessage, failedTaskMetadata);
  }

  private Optional<Duration> getMaxTaskDuration(Message<T> message) {
    var header = TaskProperties.MAX_TASK_DURATION.from(message);
    try {
      return header.map(Duration::parse);
    } catch (DateTimeException e) {
      log.warn(
          "Message {} specified invalid max task duration header: {}",
          message.getMessageId(),
          header);
    }
    return Optional.empty();
  }
}
