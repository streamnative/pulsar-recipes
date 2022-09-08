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

import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.streamnative.pulsar.recipes.task.TaskWorkerConfiguration.Builder;
import java.time.Duration;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TaskWorkerConfigurationTest {
  private static final Duration ONE_SECOND = Duration.ofSeconds(1);

  @Test
  void getters() {
    var configuration =
        TaskWorkerConfiguration.builder(Schema.STRING, Schema.INT32)
            .taskTopic("tasks")
            .subscription("subscription")
            .build();
    assertThat(configuration.getTaskSchema()).isEqualTo(Schema.STRING);
    assertThat(configuration.getResultSchema()).isEqualTo(Schema.INT32);
    assertThat(configuration.getTaskTopic()).isEqualTo("tasks");
    assertThat(configuration.getMetadataTopic()).isEqualTo("tasks-state");
    assertThat(configuration.getSubscription()).isEqualTo("subscription");
    assertThat(configuration.getMaxTaskAttempts()).isEqualTo(3);
    assertThat(configuration.getKeepAliveInterval()).isEqualTo(Duration.ofMinutes(5));
    assertThat(configuration.getTaskRedeliveryDelay()).isEqualTo(Duration.ofMinutes(5));
    assertThat(configuration.getRetention()).isEqualTo(Duration.ofDays(1));
    assertThat(configuration.getWorkerTaskTimeout()).isEqualTo(ZERO);
    assertThat(configuration.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @ParameterizedTest
  @MethodSource("validationParameters")
  void validation(UnaryOperator<Builder<?, ?>> function, boolean valid) {
    var builder =
        TaskWorkerConfiguration.builder(Schema.STRING, Schema.STRING)
            .taskTopic("tasks")
            .subscription("subscription");
    try {
      function.apply(builder).build();
      if (!valid) {
        fail("Should have thrown");
      }
    } catch (Exception e) {
      if (valid) {
        fail("Should not have thrown");
      }
    }
  }

  private static Stream<Arguments> validationParameters() {
    return Stream.of(
        args(
            builder ->
                builder
                    .stateTopic("tasks-state")
                    .maxTaskAttempts(1)
                    .taskRedeliveryDelay(ONE_SECOND)
                    .keepAliveInterval(ONE_SECOND)
                    .retention(ONE_SECOND)
                    .workerTaskTimeout(ONE_SECOND)
                    .shutdownTimeout(ONE_SECOND),
            true),
        args(builder -> builder.taskTopic(null), false),
        args(builder -> builder.taskTopic(""), false),
        args(builder -> builder.stateTopic(""), false),
        args(builder -> builder.stateTopic(null), false),
        args(builder -> builder.subscription(""), false),
        args(builder -> builder.subscription(null), false),
        args(builder -> builder.maxTaskAttempts(0), false),
        args(builder -> builder.taskRedeliveryDelay(ZERO), false),
        args(builder -> builder.taskRedeliveryDelay(null), false),
        args(builder -> builder.keepAliveInterval(ZERO), false),
        args(builder -> builder.keepAliveInterval(null), false),
        args(builder -> builder.retention(ZERO), false),
        args(builder -> builder.retention(null), false),
        args(builder -> builder.shutdownTimeout(ZERO), false),
        args(builder -> builder.shutdownTimeout(null), false),
        args(builder -> builder.workerTaskTimeout(null), false));
  }

  private static Arguments args(UnaryOperator<Builder<?, ?>> builderConsumer, boolean valid) {
    return Arguments.of(builderConsumer, valid);
  }
}
