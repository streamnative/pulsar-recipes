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
class ExpirationListenerTest {
  private static final int MAX_ATTEMPTS = 2;
  private static final long RETENTION_MILLIS = 10L;
  @Mock private TaskStateUpdater stateUpdater;
  @Mock private Clock clock;
  @Mock private Consumer<TaskProcessingState> consumer;
  @Mock private Message<TaskProcessingState> message;
  private ExpirationListener expirationListener;

  @BeforeEach
  void beforeEach() {
    expirationListener =
        new ExpirationListener(stateUpdater, clock, MAX_ATTEMPTS, RETENTION_MILLIS);
  }

  @ParameterizedTest
  @MethodSource("nonTerminalProcessingStates")
  void nonTerminalProcessingState(TaskProcessingState taskProcessingState) throws Exception {
    when(message.getValue()).thenReturn(taskProcessingState);

    expirationListener.received(consumer, message);

    verify(consumer).acknowledge(message);
    verify(stateUpdater, never()).delete(taskProcessingState);
  }

  @ParameterizedTest
  @MethodSource("terminalProcessingStates")
  void nonExpiredTerminalProcessingState(TaskProcessingState taskProcessingState) throws Exception {
    when(message.getValue()).thenReturn(taskProcessingState);
    when(clock.millis()).thenReturn(RETENTION_MILLIS - 1L);

    expirationListener.received(consumer, message);

    verify(consumer).negativeAcknowledge(message);
    verify(stateUpdater, never()).delete(taskProcessingState);
  }

  @ParameterizedTest
  @MethodSource("terminalProcessingStates")
  void expiredTerminalProcessingState(TaskProcessingState taskProcessingState) throws Exception {
    when(message.getValue()).thenReturn(taskProcessingState);
    when(clock.millis()).thenReturn(RETENTION_MILLIS + 1L);

    expirationListener.received(consumer, message);

    verify(stateUpdater).delete(taskProcessingState);
  }

  @Test
  void exceptionsIgnored() throws Exception {
    doThrow(PulsarClientException.class).when(consumer).acknowledge(message);

    assertThatNoException().isThrownBy(() -> expirationListener.received(consumer, message));
  }

  private static Stream<TaskProcessingState> nonTerminalProcessingStates() {
    return Stream.of(null, newState(), processingState(1), failedState(MAX_ATTEMPTS - 1));
  }

  private static Stream<TaskProcessingState> terminalProcessingStates() {
    return Stream.of(completedState(1), failedState(MAX_ATTEMPTS));
  }
}
