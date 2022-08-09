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

import static lombok.AccessLevel.PRIVATE;
import static org.assertj.core.api.Assertions.assertThat;

import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;

@RequiredArgsConstructor(access = PRIVATE)
class MessageAssert {
  private final Message<ProcessingState> message;

  static MessageAssert assertMessage(Message<ProcessingState> message) {
    return new MessageAssert(message);
  }

  void hasNullValue() {
    assertThat(message.getValue()).isNull();
  }

  MessageAssert hasKey(String messageId) {
    assertThat(message.getKey()).isEqualTo(messageId);
    return this;
  }

  MessageAssert hasMessageId(String messageId) {
    assertThat(message.getValue().getMessageId()).isEqualTo(messageId);
    return this;
  }

  MessageAssert hasState(State state) {
    assertThat(message.getValue().getState()).isEqualTo(state);
    return this;
  }

  MessageAssert hasCreated(long from, long to) {
    assertThat(message.getValue().getCreated()).isBetween(from, to);
    return this;
  }

  MessageAssert hasCreated(long created) {
    assertThat(message.getValue().getCreated()).isEqualTo(created);
    return this;
  }

  MessageAssert hasLastUpdated(long from, long to) {
    assertThat(message.getValue().getLastUpdated()).isBetween(from, to);
    return this;
  }

  MessageAssert hasAttempts(int attempts) {
    assertThat(message.getValue().getAttempts()).isEqualTo(attempts);
    return this;
  }

  <T> MessageAssert hasTask(T task, Schema<T> schema) {
    assertThat(message.getValue().getTask()).isEqualTo(schema.encode(task));
    return this;
  }

  <R> MessageAssert hasResult(R result, Schema<R> schema) {
    assertThat(message.getValue().getResult()).isEqualTo(schema.encode(result));
    return this;
  }

  MessageAssert hasFailureReason(String failureReason) {
    assertThat(message.getValue().getFailureReason()).isEqualTo(failureReason);
    return this;
  }
}
