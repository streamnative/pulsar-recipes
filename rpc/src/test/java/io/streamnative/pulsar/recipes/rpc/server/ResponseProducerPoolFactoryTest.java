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
package io.streamnative.pulsar.recipes.rpc.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamnative.pulsar.recipes.rpc.common.MessagingFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.pulsar.client.api.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponseProducerPoolFactoryTest {
  private final String topic = "topic";
  @Mock private MessagingFactory<?, String> messagingFactory;
  @Mock private Producer<String> producer;
  @InjectMocks private ResponseProducerPoolFactory<String> responseProducerPoolFactory;

  @Test
  void create() throws Exception {
    when(messagingFactory.responseProducer(topic)).thenReturn(producer);

    assertThat(responseProducerPoolFactory.create(topic)).isSameAs(producer);
  }

  @Test
  void createException() throws Exception {
    doThrow(IOException.class).when(messagingFactory).responseProducer(topic);

    assertThatThrownBy(() -> responseProducerPoolFactory.create(topic))
        .isInstanceOf(UncheckedIOException.class);
  }

  @Test
  void destroyObject() throws Exception {
    var pooledObject = new DefaultPooledObject<>(producer);
    responseProducerPoolFactory.destroyObject(topic, pooledObject);

    verify(producer).close();
  }

  @Test
  void wrap() {
    var pooledObject = responseProducerPoolFactory.wrap(producer);

    assertThat(pooledObject.getObject()).isSameAs(producer);
  }
}
