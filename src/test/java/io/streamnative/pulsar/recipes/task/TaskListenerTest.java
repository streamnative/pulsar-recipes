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

import static io.streamnative.pulsar.recipes.task.State.UNKNOWN;
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

import io.streamnative.pulsar.recipes.task.TaskHandler.KeepAlive;
import java.time.Clock;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskListenerTest {
  private static final int MAX_ATTEMPTS = 2;
  private static final long KEEP_ALIVE_INTERVAL_MILLIS = 10L;

  @Mock private StateView<String> stateView;
  @Mock private StateUpdater stateUpdater;
  @Mock private TaskHandler<String, String> taskHandler;
  @Mock private Clock clock;
  @Mock private Consumer<String> consumer;
  @Mock private Message<String> message;
  private TaskListener<String, String> taskListener;
  private final Schema<String> resultSchema = Schema.STRING;
  private final ProcessingState processingState = processingState(1);

  @BeforeEach
  void beforeEach() {
    taskListener =
        new TaskListener<>(
            stateView,
            stateUpdater,
            taskHandler,
            clock,
            resultSchema,
            MAX_ATTEMPTS,
            KEEP_ALIVE_INTERVAL_MILLIS);
  }

  @Test
  void newTaskSuccess() throws Exception {
    when(stateView.get(message)).thenReturn(newState());
    when(message.getValue()).thenReturn(TASK);
    when(clock.millis()).thenReturn(1L, 2L, 3L);
    ArgumentCaptor<KeepAlive> keepAliveCaptor = ArgumentCaptor.forClass(KeepAlive.class);
    when(taskHandler.handleTask(eq(TASK), keepAliveCaptor.capture())).thenReturn(RESULT);

    taskListener.received(consumer, message);

    InOrder inOrder = inOrder(stateUpdater, consumer);
    inOrder.verify(stateUpdater).update(processingState.keepAlive(1L));
    inOrder.verify(stateUpdater).update(processingState.complete(2L, ENCODED_RESULT));
    inOrder.verify(consumer).acknowledge(message);

    KeepAlive keepAlive = keepAliveCaptor.getValue();
    keepAlive.update();
    verify(stateUpdater).update(processingState.keepAlive(3L));
  }

  @Test
  void newTaskTerminalFailure() throws Exception {
    when(stateView.get(message)).thenReturn(newState());
    when(message.getValue()).thenReturn(TASK);
    when(clock.millis()).thenReturn(1L, 2L);
    when(taskHandler.handleTask(eq(TASK), any()))
        .thenThrow(new TaskException(new Exception(FAILURE_REASON)));

    taskListener.received(consumer, message);

    InOrder inOrder = inOrder(stateUpdater, consumer);
    inOrder.verify(stateUpdater).update(processingState.keepAlive(1L));
    inOrder.verify(stateUpdater).update(processingState.fail(2L, FAILURE_REASON));
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void processingTaskStale() throws Exception {
    when(stateView.get(message)).thenReturn(processingState(1));
    when(message.getValue()).thenReturn(TASK);
    when(clock.millis()).thenReturn(21L, 22L, 23L);
    when(taskHandler.handleTask(eq(TASK), any())).thenReturn(RESULT);

    taskListener.received(consumer, message);

    InOrder inOrder = inOrder(stateUpdater, consumer);
    inOrder.verify(stateUpdater).update(processingState(2).keepAlive(22L));
    inOrder.verify(stateUpdater).update(processingState(2).complete(23L, ENCODED_RESULT));
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void processingTaskStaleMaxAttempts() throws Exception {
    when(stateView.get(message)).thenReturn(processingState(2));
    when(clock.millis()).thenReturn(21L, 22L);

    taskListener.received(consumer, message);

    InOrder inOrder = inOrder(stateUpdater, consumer);
    inOrder.verify(stateUpdater).update(processingState(2).fail(22L, "Task processing is stale"));
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void processingTaskStillAlive() throws Exception {
    when(stateView.get(message)).thenReturn(processingState(1));
    when(clock.millis()).thenReturn(20L);

    taskListener.received(consumer, message);

    verify(taskHandler, never()).handleTask(eq(TASK), any());
    verify(stateUpdater, never()).update(any());
  }

  @Test
  void completedTask() throws Exception {
    when(stateView.get(message)).thenReturn(completedState(1));

    taskListener.received(consumer, message);

    verify(consumer).acknowledge(message);
    verify(taskHandler, never()).handleTask(eq(TASK), any());
    verify(stateUpdater, never()).update(any());
  }

  @Test
  void failedTaskTerminal() throws Exception {
    when(stateView.get(message)).thenReturn(failedState(2));

    taskListener.received(consumer, message);

    verify(consumer).acknowledge(message);
    verify(taskHandler, never()).handleTask(eq(TASK), any());
    verify(stateUpdater, never()).update(any());
  }

  @Test
  void failedTaskRetry() throws Exception {
    when(stateView.get(message)).thenReturn(failedState(1));
    when(message.getValue()).thenReturn(TASK);
    when(clock.millis()).thenReturn(1L, 2L);
    when(taskHandler.handleTask(eq(TASK), any())).thenReturn(RESULT);

    taskListener.received(consumer, message);

    InOrder inOrder = inOrder(stateUpdater, consumer);
    inOrder.verify(stateUpdater).update(processingState(2).keepAlive(1L));
    inOrder.verify(stateUpdater).update(processingState(2).complete(2L, ENCODED_RESULT));
    inOrder.verify(consumer).acknowledge(message);
  }

  @Test
  void unexpectedState() throws Exception {
    when(stateView.get(message))
        .thenReturn(new ProcessingState(MESSAGE_ID, UNKNOWN, 0L, 0L, 2, ENCODED_TASK, null, null));
    when(clock.millis()).thenReturn(1L);

    taskListener.received(consumer, message);

    InOrder inOrder = inOrder(stateUpdater, consumer);
    inOrder.verify(stateUpdater).update(processingState(2).fail(1L, "Unexpected state: UNKNOWN"));
    inOrder.verify(consumer).acknowledge(message);
    verify(taskHandler, never()).handleTask(eq(TASK), any());
  }
}
