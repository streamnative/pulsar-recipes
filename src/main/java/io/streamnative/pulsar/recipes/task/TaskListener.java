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
  private final StateView<T> stateView;
  private final StateUpdater stateUpdater;
  private final TaskHandler<T, R> taskHandler;
  private final Clock clock;
  private final Schema<R> resultSchema;
  private final int maxAttempts;
  private final long keepAliveIntervalMillis;

  @Override
  public void received(Consumer<T> consumer, Message<T> message) {
    log.debug("Received: {}", message.getMessageId());
    ProcessingState processingState = stateView.get(message);
    try {
      switch (processingState.getState()) {
        case NEW:
          handleTask(consumer, message, processingState);
          break;
        case PROCESSING:
          handleProcessing(consumer, message, processingState);
          break;
        case COMPLETED:
          consumer.acknowledge(message);
          break;
        case FAILED:
          handleFailed(consumer, message, processingState);
          break;
        default:
          log.error("Unexpected state: {}", processingState);
          handleError(
              consumer,
              message,
              processingState,
              "Unexpected state: " + processingState.getState());
          break;
      }
    } catch (Throwable t) {
      log.error("Error processing task: {}", processingState, t);
      consumer.negativeAcknowledge(message);
    }
  }

  private void handleTask(Consumer<T> consumer, Message<T> message, ProcessingState processingState)
      throws PulsarClientException {
    ProcessingState newProcessingState = processingState.process(clock.millis());
    stateUpdater.update(newProcessingState);
    ProcessingState keepAliveState = newProcessingState;
    try {
      log.debug("Task processing for message {}", message.getMessageId());
      R result =
          taskHandler.handleTask(
              message.getValue(),
              () -> stateUpdater.update(keepAliveState.keepAlive(clock.millis())));
      log.debug("Task processed for message {}", message.getMessageId());
      byte[] encodedResult = resultSchema.encode(result);
      newProcessingState = newProcessingState.complete(clock.millis(), encodedResult);
      stateUpdater.update(newProcessingState);
      consumer.acknowledge(message);
    } catch (TaskException e) {
      log.error("Error while handling task: {}", newProcessingState, e);
      handleError(consumer, message, newProcessingState, e.getCause().getMessage());
    } catch (Exception e) {
      log.error("Error handling task result: {}", newProcessingState, e);
    }
  }

  private void handleProcessing(
      Consumer<T> consumer, Message<T> message, ProcessingState processingState)
      throws PulsarClientException {
    long lastUpdatedAgeMillis = clock.millis() - processingState.getLastUpdated();
    if (lastUpdatedAgeMillis > keepAliveIntervalMillis * 2) {
      if (processingState.getAttempts() < maxAttempts) {
        handleTask(consumer, message, processingState);
      } else {
        stateUpdater.update(processingState.fail(clock.millis(), "Task processing is stale"));
        consumer.acknowledge(message);
      }
    } else {
      consumer.negativeAcknowledge(message);
    }
  }

  private void handleFailed(
      Consumer<T> consumer, Message<T> message, ProcessingState processingState)
      throws PulsarClientException {
    if (processingState.getAttempts() < maxAttempts) {
      handleTask(consumer, message, processingState);
    } else {
      consumer.acknowledge(message);
    }
  }

  private void handleError(
      Consumer<T> consumer,
      Message<T> message,
      ProcessingState processingState,
      String failureReason)
      throws PulsarClientException {
    ProcessingState failedState = processingState.fail(clock.millis(), failureReason);
    stateUpdater.update(failedState);
    handleFailed(consumer, message, failedState);
  }
}
