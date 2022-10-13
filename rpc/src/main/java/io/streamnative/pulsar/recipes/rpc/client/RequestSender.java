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

import static io.streamnative.pulsar.recipes.rpc.common.MessagePropertyKeys.RESPONSE_TOPIC;

import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;

@Slf4j
@RequiredArgsConstructor
class RequestSender<REQUEST> {
  private final Producer<REQUEST> producer;
  private final String responseTopic;

  void send(String correlationId, REQUEST request) throws IOException {
    log.debug("Sending {}, {}", correlationId, producer.getTopic());
    producer
        .newMessage()
        .property(RESPONSE_TOPIC, responseTopic)
        .key(correlationId)
        .value(request)
        .send();
  }
}
