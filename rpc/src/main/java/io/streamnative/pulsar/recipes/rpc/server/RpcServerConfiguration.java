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

import static java.util.Objects.requireNonNull;
import static lombok.AccessLevel.PRIVATE;

import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Schema;

/**
 * Configures the server component of the RPC system.
 *
 * @param <REQUEST> Type describing the request to be processed.
 * @param <RESPONSE> Response type yielded from the processing of the request.
 */
@Getter
@AllArgsConstructor(access = PRIVATE)
public class RpcServerConfiguration<REQUEST, RESPONSE> {
  private final Schema<REQUEST> requestSchema;
  private final Schema<RESPONSE> responseSchema;
  private final String requestTopicsPattern;
  private final String subscription;
  private final Duration channelDiscoveryInterval;
  private final Duration responseTimeout;

  /**
   * Creates a new builder to configure an {@link RpcServer}.
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
    private String requestTopicsPattern;
    private String subscription;
    private Duration channelDiscoveryInterval = Duration.ofSeconds(30);
    private Duration responseTimeout = Duration.ofMinutes(1);

    /**
     * The request topics pattern that the server will listen for requests on.
     *
     * @param requestTopicsPattern the request topics pattern
     * @return this Builder instance
     */
    public Builder<REQUEST, RESPONSE> requestTopicsPattern(@NonNull String requestTopicsPattern) {
      this.requestTopicsPattern = requestTopicsPattern;
      return this;
    }

    /**
     * The subscription name that the server will use to subscribe to request topics with.
     *
     * @param subscription the subscription name
     * @return this Builder instance
     */
    public Builder<REQUEST, RESPONSE> subscription(@NonNull String subscription) {
      this.subscription = subscription;
      return this;
    }

    /**
     * The server will look for new request topics on this interval.
     *
     * <p>Note: This option is a fallback. To enable auto-discovery of topics that match the request
     * topics pattern, the {@code enableBrokerSideSubscriptionPatternEvaluation} option must be
     * enabled in broker.conf in the Pulsar brokers.
     *
     * <p>Since no consumers will be subscribed when the first requests are sent on the request
     * topic, they will not be persisted. This means that when the server discovers new request
     * topics these requests will not be processed. For this case a short message retention or TTL
     * policy should be configured.
     *
     * <p>The default interval is 30 seconds.
     *
     * @param channelDiscoveryInterval the channel discovery interval
     * @return this Builder instance
     */
    public Builder<REQUEST, RESPONSE> channelDiscoveryInterval(
        @NonNull Duration channelDiscoveryInterval) {
      this.channelDiscoveryInterval = channelDiscoveryInterval;
      return this;
    }

    /**
     * The server will cancel and fail the processing of any requests that exceed this timeout.
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

    public RpcServerConfiguration<REQUEST, RESPONSE> build() {
      return new RpcServerConfiguration<>(
          requestSchema,
          responseSchema,
          requireNonNull(requestTopicsPattern),
          requireNonNull(subscription),
          channelDiscoveryInterval,
          responseTimeout);
    }
  }
}
