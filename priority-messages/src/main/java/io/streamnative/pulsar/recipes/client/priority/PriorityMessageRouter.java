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

import static org.apache.pulsar.client.util.MathUtils.signSafeMod;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

/***
 * First, the partitions will be marked with different priorities, this router will send the message to the specified
 * partition according to the rules defined in {@link PriorityDefinition}. It will calculate the priority by the
 * property {@link ConstsForPriorityMessages#MSG_PROP_PRIORITY} of each message, and if there has no property in the message,
 * it will use the default priority of {@link PriorityDefinition}.
 */
public class PriorityMessageRouter implements MessageRouter {

    private final AtomicInteger roundRobinIndexer = new AtomicInteger();

    private final PriorityDefinition priorityDefinition;

    /** Cache the result of the method "priorityDefinition.calculateDefaultPriorityPartitions()" **/
    private final List<Integer> defaultPriorityPartitions;

    public PriorityMessageRouter(PriorityDefinition priorityDefinition) {
        this.priorityDefinition = priorityDefinition;
        this.defaultPriorityPartitions = priorityDefinition.calculateDefaultPriorityPartitions();
    }

    @Override
    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        int priority;
        try {
            priority = Integer.valueOf(msg.getProperties().get(ConstsForPriorityMessages.MSG_PROP_PRIORITY));
        } catch (NumberFormatException | NullPointerException ex){
            priority = priorityDefinition.getDefaultPriority();
        }
        List<Integer> registeredPartitions = priorityDefinition.getRegisteredPartitions(priority);
        if (registeredPartitions != null) {
            return roundRobinChoosePartition(registeredPartitions);
        }
        return roundRobinChoosePartition(defaultPriorityPartitions);
    }

    private int roundRobinChoosePartition(List<Integer> partitions) {
        if (partitions.size() == 1){
            return partitions.get(0);
        }
        int index = signSafeMod(roundRobinIndexer.incrementAndGet(), partitions.size());
        return partitions.get(index);
    }
}
