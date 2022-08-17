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
  private final TaskMetadataView<T> taskMetadataView;
  private final TaskMetadataUpdater taskMetadataUpdater;
  private final TaskHandler<T, R> taskHandler;
  private final Clock clock;
  private final Schema<R> resultSchema;
  private final int maxTaskAttempts;
  private final long keepAliveIntervalMillis;

  @Override
  public void received(Consumer<T> taskConsumer, Message<T> taskMessage) {
    log.debug("Received: {}", taskMessage.getMessageId());
    TaskMetadata metadata = taskMetadataView.get(taskMessage);
    try {
      switch (metadata.getState()) {
        case NEW:
          handleTask(taskConsumer, taskMessage, metadata);
          break;
        case PROCESSING:
          handleProcessing(taskConsumer, taskMessage, metadata);
          break;
        case COMPLETED:
          taskConsumer.acknowledge(taskMessage);
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
      log.error("Error processing task: {}", metadata, t);
      taskConsumer.negativeAcknowledge(taskMessage);
    }
  }

  private void handleTask(Consumer<T> consumer, Message<T> taskMessage, TaskMetadata taskMetadata)
      throws PulsarClientException {
    TaskMetadata updatedMetadata = taskMetadata.process(clock.millis());
    taskMetadataUpdater.update(updatedMetadata);
    TaskMetadata keepAliveState = updatedMetadata;
    try {
      log.debug("Task processing for message {}", taskMessage.getMessageId());
      R result =
          taskHandler.handleTask(
              taskMessage.getValue(),
              () -> taskMetadataUpdater.update(keepAliveState.keepAlive(clock.millis())));
      log.debug("Task processed for message {}", taskMessage.getMessageId());
      byte[] encodedResult = resultSchema.encode(result);
      updatedMetadata = updatedMetadata.complete(clock.millis(), encodedResult);
      taskMetadataUpdater.update(updatedMetadata);
      consumer.acknowledge(taskMessage);
    } catch (TaskException e) {
      log.error("Error while handling task: {}", updatedMetadata, e);
      handleError(consumer, taskMessage, updatedMetadata, e.getCause().getMessage());
    } catch (Exception e) {
      log.error("Error handling task result: {}", updatedMetadata, e);
    }
  }

  private void handleProcessing(Consumer<T> consumer, Message<T> message, TaskMetadata taskMetadata)
      throws PulsarClientException {
    long lastUpdatedAgeMillis = clock.millis() - taskMetadata.getLastUpdated();
    if (lastUpdatedAgeMillis > keepAliveIntervalMillis * 2) {
      if (taskMetadata.getAttempts() < maxTaskAttempts) {
        handleTask(consumer, message, taskMetadata);
      } else {
        taskMetadataUpdater.update(taskMetadata.fail(clock.millis(), "Task processing is stale"));
        consumer.acknowledge(message);
      }
    } else {
      consumer.negativeAcknowledge(message);
    }
  }

  private void handleFailed(
      Consumer<T> taskConsumer, Message<T> taskMessage, TaskMetadata taskMetadata)
      throws PulsarClientException {
    if (taskMetadata.getAttempts() < maxTaskAttempts) {
      handleTask(taskConsumer, taskMessage, taskMetadata);
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
    TaskMetadata failedTaskMetadata = taskMetadata.fail(clock.millis(), failureReason);
    taskMetadataUpdater.update(failedTaskMetadata);
    handleFailed(taskConsumer, taskMessage, failedTaskMetadata);
  }
}
