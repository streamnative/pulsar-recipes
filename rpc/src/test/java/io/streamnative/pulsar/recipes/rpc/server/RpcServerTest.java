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

import static org.mockito.Mockito.verify;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.pulsar.client.api.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RpcServerTest {
  @Mock private Consumer<?> requestConsumer;
  @Mock private GenericKeyedObjectPool<?, ?> responseProducerPool;

  @Test
  void close() throws Exception {
    var rpcServer = new RpcServer(requestConsumer, responseProducerPool);

    rpcServer.close();

    verify(requestConsumer).close();
    verify(responseProducerPool).close();
  }
}
