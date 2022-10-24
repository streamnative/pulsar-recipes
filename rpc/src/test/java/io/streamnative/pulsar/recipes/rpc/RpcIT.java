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
package io.streamnative.pulsar.recipes.rpc;

import static io.streamnative.pulsar.recipes.test.SingletonPulsarContainer.pulsar;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

import io.streamnative.pulsar.recipes.rpc.client.RpcClient;
import io.streamnative.pulsar.recipes.rpc.client.RpcClientConfiguration;
import io.streamnative.pulsar.recipes.rpc.server.RpcServer;
import io.streamnative.pulsar.recipes.rpc.server.RpcServerConfiguration;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.Value;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

public class RpcIT {
  @Test
  void test() throws Exception {
    var requestSchema = Schema.JSON(TestRequest.class);
    var responseSchema = Schema.JSON(TestResponse.class);

    var pulsarClient = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();

    var topicBase = randomUUID().toString();

    var serverConfiguration =
        RpcServerConfiguration.builder(requestSchema, responseSchema)
            .requestTopicsPattern(".+" + topicBase + "-request")
            .subscription("subscription")
            .channelDiscoveryInterval(Duration.ofSeconds(2))
            .responseTimeout(Duration.ofSeconds(10))
            .build();

    Function<TestRequest, CompletableFuture<TestResponse>> function =
        request -> completedFuture(new TestResponse(request.getValue() + "bar"));

    var clientConfiguration =
        RpcClientConfiguration.builder(requestSchema, responseSchema)
            .requestTopic(topicBase + "-request")
            .responseTopic(topicBase + "-response")
            .subscription("subscription")
            .responseTimeout(Duration.ofSeconds(15))
            .build();

    try (var ignored = RpcServer.create(pulsarClient, function, serverConfiguration);
        var client = RpcClient.create(pulsarClient, clientConfiguration)) {

      var testRequest = new TestRequest("foo");
      var future = client.execute(testRequest);
      var testResponse = future.get();

      assertThat(testResponse.getValue()).isEqualTo("foobar");
    }
  }

  @Value
  public static class TestRequest {
    String value;
  }

  @Value
  public static class TestResponse {
    String value;
  }
}
