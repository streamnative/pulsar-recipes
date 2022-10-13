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
package io.streamnative.pulsar.recipes.rpc.client;

import static io.streamnative.pulsar.recipes.rpc.common.MessagePropertyKeys.ERROR_MESSAGE;

import io.streamnative.pulsar.recipes.rpc.common.AutoAcknowledgeMessageListener;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;

@Slf4j
@RequiredArgsConstructor
class ResponseListener<RESPONSE> extends AutoAcknowledgeMessageListener<RESPONSE> {
  private final Map<String, Context<RESPONSE>> contexts;

  @Override
  protected void onReceive(Message<RESPONSE> message) {
    var correlationId = message.getKey();
    log.debug("Received {}", correlationId);

    var context = contexts.remove(correlationId);
    if (context == null) {
      log.warn("Future for correlationId [{}] not found", correlationId);
    } else {
      context.getTimeout().cancel(false);

      var future = context.getResponse();
      var errorMessage = message.getProperty(ERROR_MESSAGE);
      if (errorMessage != null) {
        future.completeExceptionally(new Exception(errorMessage));
      } else {
        future.complete(message.getValue());
      }
    }
  }
}
