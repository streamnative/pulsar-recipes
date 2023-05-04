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
package io.streamnative.pulsar.recipes.client.priority;

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

public class PriorityMessageSample {

    public static void main(String[] args) throws Exception{

        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://broker.example.com:6650/").build();

        final String topicName = "persistent://public/default/tp_1";
        final String subName = "sub_1";
        final int partitionCountOfTopic = 5;

        // Create a PriorityDefinition to declare the priority of each partition.
        PriorityDefinition priorityDefinition = new PriorityDefinition(partitionCountOfTopic);
        // Mark partition [0,1] has high priority.
        priorityDefinition.registerPriority(0, 0, 1);

        // Create a PriorityMessageRouter, it will send the message to the specified partition according to the rules
        // defined in PriorityDefinition.
        PriorityMessageRouter priorityMessageRouter = new PriorityMessageRouter(priorityDefinition);

        // Create a Producer with priorityMessageRouter.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .messageRouter(priorityMessageRouter)
                .enableBatching(false)// Highlight: current solution does not support batch sending.
                .topic(topicName)
                .create();

        // Send a message with high priority.
        producer.newMessage().property(ConstsForPriorityMessages.MSG_PROP_PRIORITY, "0")
                .value("msg with high priority").send();

        // Send a message with the default priority.
        producer.newMessage().value("msg with default priority").send();

        // Create a PriorityConsumerInterceptor, It helps to make high-priority messages processed faster, but does
        // not guarantee accuracy.
        final PriorityConsumerInterceptor priorityConsumerInterceptor = new PriorityConsumerInterceptor<>();

        // Create a consumer with PriorityConsumerInterceptor.
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topicName)
                .receiverQueueSize(100)
                .subscriptionName(subName)
                .intercept(priorityConsumerInterceptor)
                .subscribe();
        // Consume messages.
        Message<Integer> msg = consumer.receive(1, TimeUnit.SECONDS);
        consumer.acknowledge(msg);
    }
}
