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

import static io.streamnative.pulsar.recipes.rpc.common.RpcConstants.ERROR_MESSAGE;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;

@Slf4j
@RequiredArgsConstructor
class ResponseSender<RESPONSE> {
  private final KeyedObjectPool<String, Producer<RESPONSE>> pool;

  @SneakyThrows
  void sendResponse(String topic, String correlationId, RESPONSE response) {
    sendError(topic, correlationId, b -> b.value(response));
  }

  @SneakyThrows
  void sendError(String topic, String correlationId, String errorMessage) {
    sendError(topic, correlationId, b -> b.property(ERROR_MESSAGE, errorMessage).value(null));
  }

  @SneakyThrows
  private void sendError(
      String topic,
      String correlationId,
      java.util.function.Consumer<TypedMessageBuilder<RESPONSE>> consumer) {
    log.debug("Sending {}", correlationId);
    Producer<RESPONSE> producer = pool.borrowObject(topic);
    try {
      var builder = producer.newMessage().key(correlationId);
      consumer.accept(builder);
      builder.send();
    } finally {
      pool.returnObject(topic, producer);
    }
  }
}
