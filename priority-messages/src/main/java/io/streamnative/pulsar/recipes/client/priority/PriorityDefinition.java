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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.Getter;

/***
 * Declare the priority of each partition.
 */
public class PriorityDefinition {

    /** Which partitions are declared in each priority. **/
    private final Map<Integer/* priority */, List<Integer>/* partitions */> mapping = new TreeMap<>();
    @Getter
    /** The priority of partition which is not defined in {@link #mapping}. **/
    private final int defaultPriority;
    /** Total partition count of the topic. **/
    private final int partitionCount;

    public PriorityDefinition(int partitionCount, int defaultPriority) {
        this.defaultPriority = defaultPriority;
        this.partitionCount = partitionCount;
    }

    public PriorityDefinition(int partitionCount) {
        this(partitionCount, Integer.MAX_VALUE);
    }

    /**
     * Register the priority of partitions, lower is faster.
     */
    public void registerPriority(int priority, int...partitions) {
        for (int p : partitions) {
            registerPriority(priority, p);
        }
    }

    /**
     * Register the priority of a partition, lower is faster.
     */
    public void registerPriority(int priority, int partition) {
        // If the partition has already registered, remove it.
        mapping.values().stream().forEach(l -> l.remove((Object) partition));
        // Put the value.
        mapping.computeIfAbsent(priority, p -> new ArrayList<>());
        mapping.get(priority).add(partition);
        // If there has an empty value, remove it;
        Iterator<Map.Entry<Integer, List<Integer>>> iterator = mapping.entrySet().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getValue().isEmpty()) {
                iterator.remove();
            }
        }
    }

    /**
     * Get partitions that has clearly prioritized.
     */
    public List<Integer> getRegisteredPartitions(int priority) {
        return mapping.get(priority);
    }

    /**
     * Calculate the partitions whose priority is not explicitly stated.
     */
    public List<Integer> calculateDefaultPriorityPartitions() {
        List<Integer> partitionsHasPriority = mapping.values().stream()
                .flatMap(l -> l.stream()).collect(Collectors.toList());
        List<Integer> defaultPriorityPartitions = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++){
            if (!partitionsHasPriority.contains(i)) {
                defaultPriorityPartitions.add(i);
            }
        }
        return defaultPriorityPartitions;
    }
}
