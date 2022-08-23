
# Pulsar Recipes

[//]: # (TODO README badges https://shields.io/)
[//]: # (TODO README summary description)

## Recipes

### Long Running Tasks

[//]: # (TODO README Long Running Tasks description)

#### Terminology
* **Process**: An actionable implementation of work that takes input and yields an output.
* **Task**: A specific set of inputs that will have a process applied to them to yield a result.
* **Metadata**: Data pertaining to the execution of a process on a given task.
* **Worker**: A node that consumes tasks and processes them.
* **Client**: A participant that submits tasks for processing and consumes the results.

#### Methodology
* Tasks are submitted to a worker queue and are consumed by tasks workers for processing. Tasks
  are ACKed when there is no further processing to be performed on them. Tasks are NACKed in the
  event of a processing failure. There are intentionally no ACK timeouts.
* The state of tasks is tracked by task metadata entries. Metadata is updated whenever there is a
  task state transition, and also with a periodical keep-alive while the task is running.


#### Example

The following example demonstrates a (not-so-long-running) greeter process.

##### Shared data structures

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

##### Client

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .build();

Producer<Task> taskProducer = client.newProducer(Schema.JSON(Task.class))
    .topic("tasks")
    .enableBatching(false)
    .create();

Consumer<TaskMetadata> metadataConsumer = client.newConsumer(Schema.JSON(TaskMetadata.class))
    .topic("tasks-metadata")
    .subscriptionName("results-subscription")
    .subscribe();

String messageId = taskProducer.send(new Task("Dave")).toString();

Schema<Result> schema = Schema.JSON(Result.class);

while (true) {
    Message<TaskMetadata> message = metadataConsumer.receive();
        TaskMetadata taskMetadata = message.getValue();
    if (taskMetadata.getMessageId().equals(messageId)
        && taskMetadata.getState() == State.COMPLETED) {
        Result result = schema.decode(taskMetadata.getResult());
    }
}
```

##### Worker

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .build();

TaskProcessor<Task, Result> process = process -> new Result("Hello " + process.getName());

TaskWorkerConfiguration<Task, Result> configuration = TaskWorkerConfiguration
    .builder(Schema.JSON(Task.class), Schema.JSON(Result.class))
    .taskTopic("tasks")
    .subscription("subscription")
    .build();

TaskWorker worker = TaskWorker.create(client, process, configuration);
```

## Build

Requirements:

* JDK 8
* Maven 3.8.6+

Common build actions:

| Action                          | Command                                  |
|---------------------------------|------------------------------------------|
| Full build and test             | `mvn clean verify`                       |
| Skip tests                      | `mvn clean verify -DskipTests`           |           
| Skip Jacoco test coverage check | `mvn clean verify -Djacoco.skip`         |
| Skip Checkstyle standards check | `mvn clean verify -Dcheckstyle.skip`     |
| Skip Spotless formatting check  | `mvn clean verify -Dspotless.check.skip` | 
| Format code                     | `mvn spotless:apply`                     |
| Generate license headers        | `mvn license:format`                     |

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
