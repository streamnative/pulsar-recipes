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

import static java.util.Objects.requireNonNull;
import static lombok.AccessLevel.PRIVATE;

import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Schema;

/**
 * Configures the client component of the RPC system.
 *
 * @param <REQUEST> Type describing the request to be processed.
 * @param <RESPONSE> Response type yielded from the processing of the request.
 */
@Getter
@AllArgsConstructor(access = PRIVATE)
public class RpcClientConfiguration<REQUEST, RESPONSE> {
  private final Schema<REQUEST> requestSchema;
  private final Schema<RESPONSE> responseSchema;
  private final String requestTopic;
  private final String responseTopic;
  private final String subscription;
  private final Duration responseTimeout;

  /**
   * Creates a new builder to configure an {@link RpcClient}.
   *
   * @param requestSchema the schema used for requests
   * @param responseSchema the schema used for responses
   * @return a new builder
   * @param <REQUEST> Type describing the request to be processed.
   * @param <RESPONSE> Response type yielded from the processing of the request.
   */
  public static <REQUEST, RESPONSE> Builder<REQUEST, RESPONSE> builder(
      @NonNull Schema<REQUEST> requestSchema, @NonNull Schema<RESPONSE> responseSchema) {
    return new Builder<>(requestSchema, responseSchema);
  }

  @RequiredArgsConstructor(access = PRIVATE)
  public static class Builder<REQUEST, RESPONSE> {
    private final Schema<REQUEST> requestSchema;
    private final Schema<RESPONSE> responseSchema;
    private String requestTopic;
    private String responseTopic;
    private String subscription;
    private Duration responseTimeout = Duration.ofMinutes(1);

    /**
     * The topic that the client will send request on.
     *
     * @param requestTopic the request topic
     * @return this Builder instance
     */
    public Builder<REQUEST, RESPONSE> requestTopic(@NonNull String requestTopic) {
      this.requestTopic = requestTopic;
      return this;
    }

    /**
     * The topic that the client will listen for responses on.
     *
     * @param responseTopic the response topic
     * @return this Builder instance
     */
    public Builder<REQUEST, RESPONSE> responseTopic(@NonNull String responseTopic) {
      this.responseTopic = responseTopic;
      return this;
    }

    /**
     * The subscription name that the client will use to subscribe to the response topic with.
     *
     * @param subscription the subscription name
     * @return this Builder instance
     */
    public Builder<REQUEST, RESPONSE> subscription(@NonNull String subscription) {
      this.subscription = subscription;
      return this;
    }

    /**
     * The client will abandon the tracking of responses that exceed this timeout.
     *
     * <p>The default timeout is 1 minute.
     *
     * @param responseTimeout the response timeout
     * @return this Builder instance
     */
    public Builder<REQUEST, RESPONSE> responseTimeout(@NonNull Duration responseTimeout) {
      this.responseTimeout = responseTimeout;
      return this;
    }

    public RpcClientConfiguration<REQUEST, RESPONSE> build() {
      return new RpcClientConfiguration<>(
          requestSchema,
          responseSchema,
          requireNonNull(requestTopic),
          requireNonNull(responseTopic),
          requireNonNull(subscription),
          responseTimeout);
    }
  }
}
