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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

@Slf4j
@RequiredArgsConstructor
public class TaskListener<T, R> implements MessageListener<T> {
  private final TaskStateView<T> taskStateView;
  private final TaskStateUpdater taskStateUpdater;
  private final TaskHandler<T, R> taskHandler;
  private final Clock clock;
  private final Schema<R> resultSchema;
  private final int maxAttempts;
  private final long keepAliveIntervalMillis;

  @Override
  public void received(Consumer<T> consumer, Message<T> message) {
    log.debug("Received: {}", message.getMessageId());
    TaskProcessingState taskProcessingState = taskStateView.get(message);
    try {
      switch (taskProcessingState.getState()) {
        case NEW:
          handleTask(consumer, message, taskProcessingState);
          break;
        case PROCESSING:
          handleProcessing(consumer, message, taskProcessingState);
          break;
        case COMPLETED:
          consumer.acknowledge(message);
          break;
        case FAILED:
          handleFailed(consumer, message, taskProcessingState);
          break;
        default:
          log.error("Unexpected state: {}", taskProcessingState);
          handleError(
              consumer,
              message,
              taskProcessingState,
              "Unexpected state: " + taskProcessingState.getState());
          break;
      }
    } catch (Throwable t) {
      log.error("Error processing task: {}", taskProcessingState, t);
      consumer.negativeAcknowledge(message);
    }
  }

  private void handleTask(
      Consumer<T> consumer, Message<T> message, TaskProcessingState taskProcessingState)
      throws PulsarClientException {
    TaskProcessingState updatedTaskProcessingState = taskProcessingState.process(clock.millis());
    taskStateUpdater.update(updatedTaskProcessingState);
    TaskProcessingState keepAliveState = updatedTaskProcessingState;
    try {
      log.debug("Task processing for message {}", message.getMessageId());
      R result =
          taskHandler.handleTask(
              message.getValue(),
              () -> taskStateUpdater.update(keepAliveState.keepAlive(clock.millis())));
      log.debug("Task processed for message {}", message.getMessageId());
      byte[] encodedResult = resultSchema.encode(result);
      updatedTaskProcessingState =
          updatedTaskProcessingState.complete(clock.millis(), encodedResult);
      taskStateUpdater.update(updatedTaskProcessingState);
      consumer.acknowledge(message);
    } catch (TaskException e) {
      log.error("Error while handling task: {}", updatedTaskProcessingState, e);
      handleError(consumer, message, updatedTaskProcessingState, e.getCause().getMessage());
    } catch (Exception e) {
      log.error("Error handling task result: {}", updatedTaskProcessingState, e);
    }
  }

  private void handleProcessing(
      Consumer<T> consumer, Message<T> message, TaskProcessingState taskProcessingState)
      throws PulsarClientException {
    long lastUpdatedAgeMillis = clock.millis() - taskProcessingState.getLastUpdated();
    if (lastUpdatedAgeMillis > keepAliveIntervalMillis * 2) {
      if (taskProcessingState.getAttempts() < maxAttempts) {
        handleTask(consumer, message, taskProcessingState);
      } else {
        taskStateUpdater.update(
            taskProcessingState.fail(clock.millis(), "Task processing is stale"));
        consumer.acknowledge(message);
      }
    } else {
      consumer.negativeAcknowledge(message);
    }
  }

  private void handleFailed(
      Consumer<T> consumer, Message<T> message, TaskProcessingState taskProcessingState)
      throws PulsarClientException {
    if (taskProcessingState.getAttempts() < maxAttempts) {
      handleTask(consumer, message, taskProcessingState);
    } else {
      consumer.acknowledge(message);
    }
  }

  private void handleError(
      Consumer<T> consumer,
      Message<T> message,
      TaskProcessingState taskProcessingState,
      String failureReason)
      throws PulsarClientException {
    TaskProcessingState failedTaskProcessingState =
        taskProcessingState.fail(clock.millis(), failureReason);
    taskStateUpdater.update(failedTaskProcessingState);
    handleFailed(consumer, message, failedTaskProcessingState);
  }
}
