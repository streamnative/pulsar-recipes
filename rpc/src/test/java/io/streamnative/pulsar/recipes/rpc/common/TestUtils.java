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
package io.streamnative.pulsar.recipes.rpc.common;

import static org.mockito.Mockito.mock;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.mockito.stubbing.Answer;

public final class TestUtils {
  private TestUtils() {}
  /**
   * Returns a mock {@link TypedMessageBuilder} with the default behaviour for the builder style
   * methods to return the mock to simplify stubbing.
   *
   * @return a mock {@link TypedMessageBuilder}
   * @param <T> the message type
   */
  @SuppressWarnings("unchecked")
  public static <T> TypedMessageBuilder<T> mockTypedMessageBuilder() {
    return builderMock(TypedMessageBuilder.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> ProducerBuilder<T> mockProducerBuilder() {
    return builderMock(ProducerBuilder.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> ConsumerBuilder<T> mockConsumerBuilder() {
    return builderMock(ConsumerBuilder.class);
  }

  public static <T> T builderMock(Class<T> builderClass) {
    return mock(builderClass, builderAnswer(builderClass));
  }

  private static <T> Answer<?> builderAnswer(Class<T> builderClass) {
    return invocation -> {
      var returnType = invocation.getMethod().getReturnType();
      if (builderClass.equals(returnType)) {
        return invocation.getMock();
      }
      return null;
    };
  }
}
