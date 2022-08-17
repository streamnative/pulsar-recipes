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

import static io.streamnative.pulsar.recipes.task.TestUtils.MESSAGE_ID;
import static io.streamnative.pulsar.recipes.task.TestUtils.processingState;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskMetadataUpdaterTest {
  @Mock private Producer<TaskMetadata> producer;
  @Mock private TypedMessageBuilder<TaskMetadata> typedMessageBuilder;
  @InjectMocks private TaskMetadataUpdater metadataUpdater;

  @ParameterizedTest
  @MethodSource("methods")
  void test(Method method, boolean nullValue) throws PulsarClientException {
    TaskMetadata taskMetadata = processingState(1);
    TaskMetadata value = nullValue ? null : taskMetadata;

    when(producer.newMessage()).thenReturn(typedMessageBuilder);
    when(typedMessageBuilder.key(MESSAGE_ID)).thenReturn(typedMessageBuilder);
    when(typedMessageBuilder.value(value)).thenReturn(typedMessageBuilder);

    method.invoke(metadataUpdater, taskMetadata);

    InOrder inOrder = inOrder(typedMessageBuilder);
    inOrder.verify(typedMessageBuilder).key(MESSAGE_ID);
    inOrder.verify(typedMessageBuilder).value(value);
    inOrder.verify(typedMessageBuilder).send();
  }

  private static Stream<Arguments> methods() {
    return Stream.of(
        Arguments.of((Method) TaskMetadataUpdater::update, false),
        Arguments.of((Method) TaskMetadataUpdater::delete, true));
  }

  interface Method {
    void invoke(TaskMetadataUpdater stateUpdater, TaskMetadata processingState)
        throws PulsarClientException;
  }
}
