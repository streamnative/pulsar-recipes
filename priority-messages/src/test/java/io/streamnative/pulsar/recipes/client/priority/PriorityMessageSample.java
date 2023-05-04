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
