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

import static io.streamnative.pulsar.recipes.task.TestUtils.completedState;
import static io.streamnative.pulsar.recipes.task.TestUtils.failedState;
import static io.streamnative.pulsar.recipes.task.TestUtils.newState;
import static io.streamnative.pulsar.recipes.task.TestUtils.processingState;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.stream.Stream;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskMetadataEvictionListenerTest {
  private static final int MAX_ATTEMPTS = 2;
  private static final long RETENTION_MILLIS = 10L;
  @Mock private TaskMetadataUpdater metadataUpdater;
  @Mock private Clock clock;
  @Mock private Consumer<TaskMetadata> consumer;
  @Mock private Message<TaskMetadata> message;
  private TaskMetadataEvictionListener taskMetadataEvictionListener;

  @BeforeEach
  void beforeEach() {
    taskMetadataEvictionListener =
        new TaskMetadataEvictionListener(metadataUpdater, clock, MAX_ATTEMPTS, RETENTION_MILLIS);
  }

  @ParameterizedTest
  @MethodSource("nonTerminalProcessingStates")
  void nonTerminalProcessingState(TaskMetadata taskMetadata) throws Exception {
    when(message.getValue()).thenReturn(taskMetadata);

    taskMetadataEvictionListener.received(consumer, message);

    verify(consumer).acknowledge(message);
    verify(metadataUpdater, never()).delete(taskMetadata);
  }

  @ParameterizedTest
  @MethodSource("terminalProcessingStates")
  void nonExpiredTerminalProcessingState(TaskMetadata taskMetadata) throws Exception {
    when(message.getValue()).thenReturn(taskMetadata);
    when(clock.millis()).thenReturn(RETENTION_MILLIS - 1L);

    taskMetadataEvictionListener.received(consumer, message);

    verify(consumer).negativeAcknowledge(message);
    verify(metadataUpdater, never()).delete(taskMetadata);
  }

  @ParameterizedTest
  @MethodSource("terminalProcessingStates")
  void expiredTerminalProcessingState(TaskMetadata taskMetadata) throws Exception {
    when(message.getValue()).thenReturn(taskMetadata);
    when(clock.millis()).thenReturn(RETENTION_MILLIS + 1L);

    taskMetadataEvictionListener.received(consumer, message);

    verify(metadataUpdater).delete(taskMetadata);
  }

  @Test
  void exceptionsIgnored() throws Exception {
    doThrow(PulsarClientException.class).when(consumer).acknowledge(message);

    assertThatNoException()
        .isThrownBy(() -> taskMetadataEvictionListener.received(consumer, message));
  }

  private static Stream<TaskMetadata> nonTerminalProcessingStates() {
    return Stream.of(null, newState(), processingState(1), failedState(MAX_ATTEMPTS - 1));
  }

  private static Stream<TaskMetadata> terminalProcessingStates() {
    return Stream.of(completedState(1), failedState(MAX_ATTEMPTS));
  }
}
