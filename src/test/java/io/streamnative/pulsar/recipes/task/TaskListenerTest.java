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

import static io.streamnative.pulsar.recipes.task.TaskState.UNKNOWN;
import static io.streamnative.pulsar.recipes.task.TestUtils.ENCODED_RESULT;
import static io.streamnative.pulsar.recipes.task.TestUtils.ENCODED_TASK;
import static io.streamnative.pulsar.recipes.task.TestUtils.FAILURE_REASON;
import static io.streamnative.pulsar.recipes.task.TestUtils.MESSAGE_ID;
import static io.streamnative.pulsar.recipes.task.TestUtils.RESULT;
import static io.streamnative.pulsar.recipes.task.TestUtils.TASK;
import static io.streamnative.pulsar.recipes.task.TestUtils.completedState;
import static io.streamnative.pulsar.recipes.task.TestUtils.failedState;
import static io.streamnative.pulsar.recipes.task.TestUtils.newState;
import static io.streamnative.pulsar.recipes.task.TestUtils.processingState;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamnative.pulsar.recipes.task.ProcessExecutor.KeepAlive;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskListenerTest {
  private static final int MAX_ATTEMPTS = 2;
  private static final long KEEP_ALIVE_INTERVAL_MILLIS = 10L;
  private static final Optional<Duration> NoMaxDuration = Optional.empty();

  @Mock private TaskMetadataView<String> metadataView;
  @Mock private TaskMetadataUpdater metadataUpdater;
  @Mock private ProcessExecutor<String, String> processExecutor;
  @Mock private Clock clock;
  @Mock private Consumer<String> consumer;
  @Mock private Message<String> message;
  private TaskListener<String, String> taskListener;
  private final Schema<String> resultSchema = Schema.STRING;
  private final TaskMetadata taskMetadata = processingState(1);

  @BeforeEach
  void beforeEach() {
    taskListener =
        new TaskListener<>(
            metadataView,
            metadataUpdater,
            processExecutor,
            clock,
            resultSchema,
            MAX_ATTEMPTS,
            KEEP_ALIVE_INTERVAL_MILLIS);
  }

  @Test
  void newTaskSuccess() throws Exception {
    when(metadataView.get(message)).thenReturn(newState());
    when(message.getValue()).thenReturn(TASK);
    when(clock.millis()).thenReturn(1L, 2L, 3L);
    var keepAliveCaptor = ArgumentCaptor.forClass(KeepAlive.class);
    when(processExecutor.execute(eq(TASK), eq(NoMaxDuration), keepAliveCaptor.capture()))
        .thenReturn(RESULT);

    taskListener.received(consumer, message);

    var inOrder = inOrder(metadataUpdater, consumer);
    inOrder.verify(metadataUpdater).update(taskMetadata.keepAlive(1L));
    inOrder.verify(metadataUpdater).update(taskMetadata.complete(2L, ENCODED_RESULT));
    inOrder.verify(consumer).acknowledge(message);

    var keepAlive = keepAliveCaptor.getValue();
    keepAlive.update();
    verify(metadataUpdater).update(taskMetadata.keepAlive(3L));
  }

  @Test
  void newTaskTerminalFailure() throws Exception {
    when(metadataView.get(message)).thenReturn(newState());
    when(message.getValue()).thenReturn(TASK);
    when(clock.millis()).thenReturn(1L, 2L);
    when(processExecutor.execute(eq(TASK), eq(NoMaxDuration), any()))
        .thenThrow(new ProcessException(FAILURE_REASON, new Exception(FAILURE_REASON)));

    taskListener.received(consumer, message);

    var inOrder = inOrder(metadataUpdater, consumer);
    inOrder.verify(metadataUpdater).update(taskMetadata.keepAlive(1L));
    inOrder
        .verify(metadataUpdater)
        .update(taskMetadata.fail(2L, FAILURE_REASON + ": " + FAILURE_REASON));
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void processingTaskStale() throws Exception {
    when(metadataView.get(message)).thenReturn(processingState(1));
    when(message.getValue()).thenReturn(TASK);
    when(clock.millis()).thenReturn(21L, 22L, 23L);
    when(processExecutor.execute(eq(TASK), eq(NoMaxDuration), any())).thenReturn(RESULT);

    taskListener.received(consumer, message);

    var inOrder = inOrder(metadataUpdater, consumer);
    inOrder.verify(metadataUpdater).update(processingState(2).keepAlive(22L));
    inOrder.verify(metadataUpdater).update(processingState(2).complete(23L, ENCODED_RESULT));
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void processingTaskStaleMaxAttempts() throws Exception {
    when(metadataView.get(message)).thenReturn(processingState(2));
    when(clock.millis()).thenReturn(21L, 22L);

    taskListener.received(consumer, message);

    var inOrder = inOrder(metadataUpdater, consumer);
    inOrder
        .verify(metadataUpdater)
        .update(processingState(2).fail(22L, "All attempts to process task failed."));
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void processingTaskStillAlive() throws Exception {
    when(metadataView.get(message)).thenReturn(processingState(1));
    when(clock.millis()).thenReturn(20L);

    taskListener.received(consumer, message);

    verify(processExecutor, never()).execute(eq(TASK), eq(NoMaxDuration), any());
    verify(metadataUpdater, never()).update(any());
  }

  @Test
  void completedTask() throws Exception {
    when(metadataView.get(message)).thenReturn(completedState(1));

    taskListener.received(consumer, message);

    verify(consumer).acknowledge(message);
    verify(processExecutor, never()).execute(eq(TASK), eq(NoMaxDuration), any());
    verify(metadataUpdater, never()).update(any());
  }

  @Test
  void failedTaskTerminal() throws Exception {
    when(metadataView.get(message)).thenReturn(failedState(2));

    taskListener.received(consumer, message);

    verify(consumer).acknowledge(message);
    verify(processExecutor, never()).execute(eq(TASK), eq(NoMaxDuration), any());
    verify(metadataUpdater, never()).update(any());
  }

  @Test
  void failedTaskRetry() throws Exception {
    when(metadataView.get(message)).thenReturn(failedState(1));
    when(message.getValue()).thenReturn(TASK);
    when(clock.millis()).thenReturn(1L, 2L);
    when(processExecutor.execute(eq(TASK), eq(NoMaxDuration), any())).thenReturn(RESULT);

    taskListener.received(consumer, message);

    var inOrder = inOrder(metadataUpdater, consumer);
    inOrder.verify(metadataUpdater).update(processingState(2).keepAlive(1L));
    inOrder.verify(metadataUpdater).update(processingState(2).complete(2L, ENCODED_RESULT));
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void unexpectedState() throws Exception {
    when(metadataView.get(message))
        .thenReturn(new TaskMetadata(MESSAGE_ID, UNKNOWN, 0L, 0L, 2, ENCODED_TASK, null, null));
    when(clock.millis()).thenReturn(1L);

    taskListener.received(consumer, message);

    var inOrder = inOrder(metadataUpdater, consumer);
    inOrder
        .verify(metadataUpdater)
        .update(processingState(2).fail(1L, "Unexpected state: UNKNOWN"));
    inOrder.verify(consumer).acknowledge(message);
    verify(processExecutor, never()).execute(eq(TASK), eq(NoMaxDuration), any());
  }
}
