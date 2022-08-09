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

import static io.streamnative.pulsar.recipes.task.MessageAssert.assertMessage;
import static io.streamnative.pulsar.recipes.task.State.COMPLETED;
import static io.streamnative.pulsar.recipes.task.State.FAILED;
import static io.streamnative.pulsar.recipes.task.State.PROCESSING;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

@Slf4j
// @Testcontainers
public class TaskWorkerIT {
  private static final DockerImageName pulsarImage =
      DockerImageName.parse("apachepulsar/pulsar").withTag("2.10.0");

  @Container
  private static final PulsarContainer pulsar =
      new PulsarContainer(pulsarImage) {
        @Override
        protected void configure() {
          super.configure();
          withStartupTimeout(Duration.ofMinutes(3));
        }
      };

  private final Clock clock = Clock.systemUTC();

  private PulsarClient client;
  private Producer<String> taskProducer;
  private Consumer<ProcessingState> stateConsumer;

  private void createResources(String taskTopic) throws Exception {
    client =
        PulsarClient.builder()
            // .serviceUrl(pulsar.getPulsarBrokerUrl())
            .serviceUrl("pulsar://localhost:6650")
            .build();
    taskProducer =
        client
            .newProducer(Schema.STRING)
            .topic(taskTopic)
            .enableBatching(false) // TODO very important to document
            .create();
    stateConsumer =
        client
            .newConsumer(Schema.JSON(ProcessingState.class))
            .topic(taskTopic + "-state")
            .subscriptionName(randomUUID().toString())
            .subscribe();
  }

  @AfterEach
  void afterEach() throws Exception {
    taskProducer.close();
    stateConsumer.close();
  }

  @Test
  @Timeout(30)
  void success() throws Exception {
    String taskTopic = randomUUID().toString();
    createResources(taskTopic);

    TaskProcessor<String, String> taskProcessor = task -> "bar";

    Configuration<String, String> configuration =
        Configuration.builder(Schema.STRING, Schema.STRING)
            .taskTopic(taskTopic)
            .subscription("subscription")
            .retention(Duration.ofSeconds(1))
            .expirationRedeliveryDelay(Duration.ofSeconds(1))
            .build();

    @SuppressWarnings("unused")
    @Cleanup
    TaskWorker ignore = TaskWorker.create(client, taskProcessor, configuration);

    long before = clock.millis();
    String messageId = taskProducer.send("foo").toString();

    Message<ProcessingState> firstMessage = nextMessage(5);
    long now = clock.millis();
    assertMessage(firstMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(PROCESSING)
        .hasCreated(before, now)
        .hasLastUpdated(before, now)
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason(null);

    Message<ProcessingState> secondMessage = nextMessage(5);
    assertMessage(secondMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(COMPLETED)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(firstMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult("bar", Schema.STRING)
        .hasFailureReason(null);

    assertMessage(nextMessage(20)).hasKey(messageId).hasNullValue();

    assertThat(nextMessage(10)).isNull();
  }

  @Test
  @Timeout(30)
  void retryWithRecovery() throws Exception {
    String taskTopic = randomUUID().toString();
    createResources(taskTopic);

    AtomicBoolean succeed = new AtomicBoolean();
    TaskProcessor<String, String> taskProcessor =
        task -> {
          if (succeed.compareAndSet(false, true)) {
            throw new Exception("failed");
          }
          return "bar";
        };

    Configuration<String, String> configuration =
        Configuration.builder(Schema.STRING, Schema.STRING)
            .taskTopic(taskTopic)
            .subscription("subscription")
            .retention(Duration.ofSeconds(1))
            .expirationRedeliveryDelay(Duration.ofSeconds(1))
            .build();

    @SuppressWarnings("unused")
    @Cleanup
    TaskWorker ignore = TaskWorker.create(client, taskProcessor, configuration);

    long before = clock.millis();
    String messageId = taskProducer.send("foo").toString();

    Message<ProcessingState> firstMessage = nextMessage(5);
    long now = clock.millis();
    assertMessage(firstMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(PROCESSING)
        .hasCreated(before, now)
        .hasLastUpdated(before, now)
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason(null);

    Message<ProcessingState> secondMessage = nextMessage(5);
    assertMessage(secondMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(FAILED)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(firstMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason("failed");

    Message<ProcessingState> thirdMessage = nextMessage(5);
    assertMessage(thirdMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(PROCESSING)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(secondMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(2)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason(null);

    Message<ProcessingState> fourthMessage = nextMessage(5);
    assertMessage(fourthMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(COMPLETED)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(thirdMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(2)
        .hasTask("foo", Schema.STRING)
        .hasResult("bar", Schema.STRING)
        .hasFailureReason(null);

    assertMessage(nextMessage(20)).hasKey(messageId).hasNullValue();

    assertThat(nextMessage(10)).isNull();
  }

  @Test
  @Timeout(30)
  void terminalFailure() throws Exception {
    String taskTopic = randomUUID().toString();
    createResources(taskTopic);

    TaskProcessor<String, String> taskProcessor =
        task -> {
          throw new Exception("failed");
        };

    Configuration<String, String> configuration =
        Configuration.builder(Schema.STRING, Schema.STRING)
            .taskTopic(taskTopic)
            .subscription("subscription")
            .maxAttempts(1)
            .retention(Duration.ofSeconds(1))
            .expirationRedeliveryDelay(Duration.ofSeconds(1))
            .build();

    @SuppressWarnings("unused")
    @Cleanup
    TaskWorker ignore = TaskWorker.create(client, taskProcessor, configuration);

    long before = clock.millis();
    String messageId = taskProducer.send("foo").toString();

    Message<ProcessingState> firstMessage = nextMessage(5);
    long now = clock.millis();
    assertMessage(firstMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(PROCESSING)
        .hasCreated(before, now)
        .hasLastUpdated(before, now)
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason(null);

    Message<ProcessingState> secondMessage = nextMessage(5);
    assertMessage(secondMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(FAILED)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(firstMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason("failed");

    assertMessage(nextMessage(20)).hasKey(messageId).hasNullValue();

    assertThat(nextMessage(10)).isNull();
  }

  private Message<ProcessingState> nextMessage(int timeout) throws Exception {
    Message<ProcessingState> message = stateConsumer.receive(timeout, SECONDS);
    if (message != null) {
      stateConsumer.acknowledge(message);
    }
    return message;
  }
}
