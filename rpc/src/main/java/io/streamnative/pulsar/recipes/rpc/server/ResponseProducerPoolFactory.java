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


import io.streamnative.pulsar.recipes.rpc.common.MessagingFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.RequiredArgsConstructor;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.pulsar.client.api.Producer;

@RequiredArgsConstructor
public class ResponseProducerPoolFactory<RESPONSE>
    extends BaseKeyedPooledObjectFactory<String, Producer<RESPONSE>> {
  private final MessagingFactory<?, RESPONSE> messagingFactory;

  @Override
  public Producer<RESPONSE> create(String topic) {
    try {
      return messagingFactory.responseProducer(topic);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void destroyObject(String key, PooledObject<Producer<RESPONSE>> pooledObject)
      throws Exception {
    pooledObject.getObject().close();
  }

  @Override
  public PooledObject<Producer<RESPONSE>> wrap(Producer<RESPONSE> producer) {
    return new DefaultPooledObject<>(producer);
  }
}
