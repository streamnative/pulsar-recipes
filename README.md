
# Pulsar Recipes

[//]: # (TODO README badges https://shields.io/)
[//]: # (TODO README summary description)

## Recipes

### Long Running Tasks

[//]: # (TODO README Long Running Tasks description)

#### Example

The following example demonstrates a (not-so-long-running) greeter task.

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

Producer<Task> producer = client.newProducer(Schema.JSON(Task.class))
    .topic("tasks")
    .enableBatching(false)
    .create();

Consumer<TaskMetadata> consumer = client.newConsumer(Schema.JSON(TaskMetadata.class))
    .topic("tasks-metadata")
    .subscriptionName("results-subscription")
    .subscribe();

String messageId = producer.send(new Task("Dave")).toString();

Schema<Result> schema = Schema.JSON(Result.class);

while(true) {
    Message<TaskProcessingState> message = consumer.receive();
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

TaskProcessor<Task, Result> taskProcessor = task -> new Result("Hello " + task.getName());

TaskWorkerConfiguration<Task, Result> configuration = TaskWorkerConfiguration
    .builder(Schema.JSON(Task.class), Schema.JSON(Result.class))
    .taskTopic("tasks")
    .subscription("subscription")
    .build();

TaskWorker worker = TaskWorker.create(client, taskProcessor, configuration);
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
