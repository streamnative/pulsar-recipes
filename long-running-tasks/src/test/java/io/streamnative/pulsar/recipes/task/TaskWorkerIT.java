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
import static io.streamnative.pulsar.recipes.task.TaskProperties.MAX_TASK_DURATION;
import static io.streamnative.pulsar.recipes.task.TaskState.COMPLETED;
import static io.streamnative.pulsar.recipes.task.TaskState.FAILED;
import static io.streamnative.pulsar.recipes.task.TaskState.PROCESSING;
import static io.streamnative.pulsar.recipes.test.SingletonPulsarContainer.pulsar;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Slf4j
public class TaskWorkerIT {
  private final Clock clock = Clock.systemUTC();

  private PulsarClient client;
  private Producer<String> taskProducer;
  private Consumer<TaskMetadata> metadataConsumer;
  private String taskTopic;

  @BeforeEach
  void beforeEach() throws Exception {
    taskTopic = randomUUID().toString();

    client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();

    taskProducer =
        client.newProducer(Schema.STRING).topic(taskTopic).enableBatching(false).create();
    metadataConsumer =
        client
            .newConsumer(Schema.JSON(TaskMetadata.class))
            .topic(taskTopic + "-state")
            .subscriptionName(randomUUID().toString())
            .subscribe();
  }

  @AfterEach
  void afterEach() throws Exception {
    if (taskProducer != null) {
      taskProducer.close();
    }
    if (metadataConsumer != null) {
      metadataConsumer.close();
    }
  }

  @Test
  @Timeout(30)
  void success() throws Exception {
    Process<String, String> process = task -> "bar";

    var configuration =
        TaskWorkerConfiguration.builder(Schema.STRING, Schema.STRING)
            .taskTopic(taskTopic)
            .subscription("subscription")
            .retention(Duration.ofSeconds(1))
            .build();

    @SuppressWarnings("unused")
    @Cleanup
    var ignore = TaskWorker.create(client, process, configuration);

    var before = clock.millis();
    var messageId =
        taskProducer
            .newMessage()
            .property(MAX_TASK_DURATION.key(), MAX_TASK_DURATION.of("PT3H"))
            .value("foo")
            .send();

    var firstMessage = nextMessage(5);
    var now = clock.millis();
    assertMessage(firstMessage)
        .hasKey(messageId.toString())
        .hasMessageId(messageId)
        .hasState(PROCESSING)
        .hasCreated(before, now)
        .hasLastUpdated(before, now)
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason(null);

    var secondMessage = nextMessage(5);
    assertMessage(secondMessage)
        .hasKey(messageId.toString())
        .hasMessageId(messageId)
        .hasState(COMPLETED)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(firstMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult("bar", Schema.STRING)
        .hasFailureReason(null);

    assertMessage(nextMessage(20)).hasKey(messageId.toString()).hasNullValue();

    assertThat(nextMessage(10)).isNull();
  }

  @Test
  @Timeout(30)
  void retryWithRecovery() throws Exception {
    var succeed = new AtomicBoolean();
    Process<String, String> process =
        task -> {
          if (succeed.compareAndSet(false, true)) {
            throw new Exception("failed");
          }
          return "bar";
        };

    var configuration =
        TaskWorkerConfiguration.builder(Schema.STRING, Schema.STRING)
            .taskTopic(taskTopic)
            .subscription("subscription")
            .retention(Duration.ofSeconds(1))
            .build();

    @SuppressWarnings("unused")
    @Cleanup
    var ignore = TaskWorker.create(client, process, configuration);

    var before = clock.millis();
    var messageId = taskProducer.send("foo").toString();

    var firstMessage = nextMessage(5);
    var now = clock.millis();
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

    var secondMessage = nextMessage(5);
    assertMessage(secondMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(FAILED)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(firstMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason("Processing error: failed");

    var thirdMessage = nextMessage(5);
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

    var fourthMessage = nextMessage(5);
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
    Process<String, String> process =
        task -> {
          throw new Exception("failed");
        };

    var configuration =
        TaskWorkerConfiguration.builder(Schema.STRING, Schema.STRING)
            .taskTopic(taskTopic)
            .subscription("subscription")
            .maxTaskAttempts(1)
            .retention(Duration.ofSeconds(1))
            .build();

    @SuppressWarnings("unused")
    @Cleanup
    var ignore = TaskWorker.create(client, process, configuration);

    var before = clock.millis();
    var messageId = taskProducer.send("foo").toString();

    var firstMessage = nextMessage(5);
    var now = clock.millis();
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

    var secondMessage = nextMessage(5);
    assertMessage(secondMessage)
        .hasKey(messageId)
        .hasMessageId(messageId)
        .hasState(FAILED)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(firstMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason("Processing error: failed");

    assertMessage(nextMessage(20)).hasKey(messageId).hasNullValue();

    assertThat(nextMessage(10)).isNull();
  }

  @Test
  @Timeout(30)
  void firstAttemptExceedsMaxDuration() throws Exception {
    Process<String, String> process =
        new Process<>() {
          private final Iterator<Integer> durations = asList(6, 1).iterator();

          @Override
          public String apply(String task) throws Exception {
            Thread.sleep(SECONDS.toMillis(durations.next()));
            return "bar";
          }
        };

    var configuration =
        TaskWorkerConfiguration.builder(Schema.STRING, Schema.STRING)
            .taskTopic(taskTopic)
            .subscription("subscription")
            .maxTaskAttempts(2)
            .retention(Duration.ofSeconds(1))
            .keepAliveInterval(Duration.ofSeconds(4))
            .build();

    @SuppressWarnings("unused")
    @Cleanup
    var ignore = TaskWorker.create(client, process, configuration);

    var before = clock.millis();
    var messageId =
        taskProducer
            .newMessage()
            .property(MAX_TASK_DURATION.key(), MAX_TASK_DURATION.of("PT3S"))
            .value("foo")
            .send();

    var firstMessage = nextMessage(5);
    var now = clock.millis();
    assertMessage(firstMessage)
        .hasKey(messageId.toString())
        .hasMessageId(messageId)
        .hasState(PROCESSING)
        .hasCreated(before, now)
        .hasLastUpdated(before, now)
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason(null);

    var secondMessage = nextMessage(20);
    assertMessage(secondMessage)
        .hasKey(messageId.toString())
        .hasMessageId(messageId)
        .hasState(FAILED)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(firstMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(1)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason("Process was cancelled: null");

    var thirdMessage = nextMessage(5);
    assertMessage(thirdMessage)
        .hasKey(messageId.toString())
        .hasMessageId(messageId)
        .hasState(PROCESSING)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(secondMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(2)
        .hasTask("foo", Schema.STRING)
        .hasResult(null, Schema.STRING)
        .hasFailureReason(null);

    var fourthMessage = nextMessage(5);
    assertMessage(fourthMessage)
        .hasKey(messageId.toString())
        .hasMessageId(messageId)
        .hasState(COMPLETED)
        .hasCreated(firstMessage.getValue().getCreated())
        .hasLastUpdated(thirdMessage.getValue().getLastUpdated(), clock.millis())
        .hasAttempts(2)
        .hasTask("foo", Schema.STRING)
        .hasResult("bar", Schema.STRING)
        .hasFailureReason(null);

    assertMessage(nextMessage(20)).hasKey(messageId.toString()).hasNullValue();

    assertThat(nextMessage(10)).isNull();
  }

  private Message<TaskMetadata> nextMessage(int timeout) throws Exception {
    var message = metadataConsumer.receive(timeout, SECONDS);
    if (message != null) {
      metadataConsumer.acknowledge(message);
    }
    return message;
  }
}
