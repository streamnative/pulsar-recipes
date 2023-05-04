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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

/***
 * Helps to make high-priority messages processed faster, but does not guarantee accuracy.
 * It calculates the priority of the message through the property {@link ConstsForPriorityMessages#MSG_PROP_PRIORITY}
 * of messages, marks the priority of each consumer through its first message, and pause the low priority consumer to
 * make the high priority message to be processed faster.
 */
@Slf4j
public class PriorityConsumerInterceptor <T> implements ConsumerInterceptor<T> {

    private final Set<String> consumerSet = Collections.synchronizedSet(new TreeSet<>());

    /** This collection collects the consumers created by the client for each partition. **/
    private final Map<Integer, Set<ConsumerImpl<T>>> consumerMapping = Collections.synchronizedMap(new TreeMap<>());

    /**
     * Calculate the priority of messages by the property {@link ConstsForPriorityMessages#MSG_PROP_PRIORITY}.
     */
    private int calculatePriority(Message<T> message) {
        String priorityString = message.getProperties().get(ConstsForPriorityMessages.MSG_PROP_PRIORITY);
        // If no property in message, means the lowest priority.
        if (StringUtils.isBlank(priorityString)) {
            return Integer.MAX_VALUE;
        }
        if (!StringUtils.isNumeric(priorityString)) {
            log.warn("The priority prop [{}] of message is not a number.", ConstsForPriorityMessages.MSG_PROP_PRIORITY);
            return Integer.MAX_VALUE;
        }
        return Integer.valueOf(priorityString);
    }

    /**
     * If the high priority partition has many messages, it suspends receiving messages from the low priority partition.
     * Note: If some messages with low priority have already been received into the memory, they will be consumed as
     * high level priority.
     */
    private synchronized void triggerPauseOrResume() {
        if (consumerMapping.size() == 1){
            return;
        }

        boolean doPause = false;
        for (Map.Entry<Integer, Set<ConsumerImpl<T>>> entry : consumerMapping.entrySet()){
            if (doPause){
                entry.getValue().forEach(ConsumerImpl::pause);
                continue;
            }
            entry.getValue().forEach(ConsumerImpl::resume);
            long messageCountInMemory = entry.getValue().stream().map(ConsumerImpl::getTotalIncomingMessages)
                    .reduce((s1, s2) -> s1 + s2).get();
            if (messageCountInMemory > 50){
                doPause = true;
            }
        }
    }

    @Override
    public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
        // This method is just used to collect the consumers created by the client for each partition.
        ConsumerImpl<T> consumerImpl = (ConsumerImpl<T>) consumer;
        if (consumerSet.contains(consumerImpl.getTopic())){
            return message;
        }
        // Registry consumer.
        int priority = calculatePriority(message);
        consumerMapping.computeIfAbsent(priority, i -> Collections.synchronizedSet(new HashSet<>()));
        consumerMapping.get(priority).add(consumerImpl);
        consumerSet.add(consumerImpl.getTopic());
        return message;
    }

    @Override
    public void close() {
        consumerMapping.clear();
        consumerSet.clear();
    }

    @Override
    public void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception) {
        triggerPauseOrResume();
    }

    @Override
    public void onAcknowledgeCumulative(Consumer<T> consumer, MessageId messageId, Throwable exception) {
        triggerPauseOrResume();
    }

    @Override
    public void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> messageIds) {
        triggerPauseOrResume();
    }

    @Override
    public void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> messageIds) {
        triggerPauseOrResume();
    }
}
