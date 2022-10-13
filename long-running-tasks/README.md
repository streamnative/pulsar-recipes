# Long Running Tasks

A distributed work queue for long-running tasks. A particular challenge with long-running tasks is that such a system is
more susceptible to consumer connection interruptions. These interruptions would result in message redeliveries and
duplicate processing of the tasks. This system seeks to avoid this problem by maintaining a global task state in a
separate topic.

The work scheduler supports task retries, detection of non-progressing work, and the cancellation of work that has
exceeded a time-based processing budget.

## Terminology

* **Process**: An actionable implementation of work that takes input and yields an output.
* **Task**: A specific set of inputs on which a process is applied to yield a result.
* **Metadata**: Data pertaining to the execution of a process on a given task.
* **Worker**: A node that consumes tasks and processes them.
* **Client**: A participant that submits tasks for processing and consumes the results.

## Methodology

* Tasks
  * Tasks are submitted to a worker queue and are consumed by tasks workers for processing.
  * State is maintained for a task as a sequence of `TaskMetadata` entries in a separate topic.
  * Tasks are ACKed when there is no further processing to be performed on them.
  * Tasks are NACKed in the event of an unexpected system failure.
  * If a worker fails, the shared nature of the subscription will allow the task to be consumed by another worker.
* Task Metadata
  * The state of tasks is tracked by `TaskMetadata` entries.
  * Metadata is updated whenever there is a state transition.
  * Metadata is also updated by a periodical keep-alive while the task is running.
  * When the task is in a terminal state, the metadata is scheduled for reconsumption and subsequent eviction.

## Recommendations

* Enable compaction on the task metadata topic to ensure optimum worker start up time.
* Set a message retention on the task metadata topic.
  * If compaction is not enabled then this needs to be long enough for task metadata to survive until tasks reach a
    terminal state.
  * With compaction enabled this should be long enough to survive to the next compaction execution.

### Examples

```java
PulsarAdmin admin = PulsarAdmin.builder()
    .serviceHttpUrl("http://localhost:8080")
    .build();

admin.topicPolicies()
    .setRetention(topic, new RetentionPolicies(retentionTimeInMinutes, retentionSizeInMB));

admin.topicPolicies()
    .setCompactionThreshold(topic, compactionThreshold);
```

## Limitations

* You task producer must not use batching to ensure the individual allocation of tasks to workers. To do this use
  `enableBatching(false)`.
* In the event of a non-progressing worker, the tasks allocated to that worker will not be released to another worker
  and the task will remain stuck. This can be alleviated by setting `TaskConfiguration.workerTaskTimeout` to a value
  greater than the longest expected running time of any task (including all retries).

## Example

The following example demonstrates a (not-so-long-running) greeter process.

### Shared data structures

```java
@Value
class Task {
    String name;
}

@Value
class Result {
    String greeting;
}
```

### Client

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .build();

// Producer to submit tasks
Producer<Task> taskProducer = client.newProducer(Schema.JSON(Task.class))
    .topic("tasks")
    // Essential for individual allocation of tasks (see limitation noted above).    
    .enableBatching(false)
    .create();

// Consumer to fetch task processing results
Consumer<TaskMetadata> metadataConsumer = client.newConsumer(Schema.JSON(TaskMetadata.class))
    .topic("tasks-metadata")
    .subscriptionName("results-subscription")
    .subscribe();

// Create a new task and submit it
MessageId messageId = taskProducer
    .newMessage()
    // Cancel task tries if they don't complete in 1 hour (default is ∞)    
    .property(
        TaskProperties.MAX_TASK_DURATION.key(),
        TaskProperties.MAX_TASK_DURATION.of("PT1H")
    )
    .value(new Task("Dave"))
    .send();

// Listen for results
Schema<Result> schema = Schema.JSON(Result.class);
while (true) {
    Message<TaskMetadata> message = metadataConsumer.receive();
        TaskMetadata taskMetadata = message.getValue();
    if (taskMetadata.getMessageId().equals(messageId.toString())
        && taskMetadata.getState() == State.COMPLETED) {
        Result result = schema.decode(taskMetadata.getResult());
    }
}
```

### Worker

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .build();

TaskProcessor<Task, Result> process = task -> new Result("Hello " + task.getName());

TaskWorkerConfiguration<Task, Result> configuration = TaskWorkerConfiguration
    .builder(Schema.JSON(Task.class), Schema.JSON(Result.class))
    .taskTopic("tasks")
    .subscription("subscription")
    // Try each task 5 times (default is 3)
    .maxTaskAttempts(5)    
    // Update the task status every minute (default is 5)
    .keepAliveInterval(Duration.ofMinutes(1))    
    // Free task for reconsumption after 30 minutes (default is ∞) - may result in duplicate work
    .workerTaskTimeout(Duration.ofMinutes(30))
    .build();

TaskWorker worker = TaskWorker.create(client, process, configuration);
```

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
