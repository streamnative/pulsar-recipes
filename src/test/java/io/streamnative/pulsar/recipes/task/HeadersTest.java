package io.streamnative.pulsar.recipes.task;

import static io.streamnative.pulsar.recipes.task.Headers.MAX_TASK_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HeadersTest {

  @Mock Message<String> message;

  @Test
  void maxTaskDuration_key() {
    assertThat(MAX_TASK_DURATION.key()).isEqualTo("MAX_TASK_DURATION");
  }

  @Test
  void maxTaskDuration_from() {
    when(message.getProperty(MAX_TASK_DURATION.key())).thenReturn("P1H");
    assertThat(MAX_TASK_DURATION.from(message)).isEqualTo(Optional.of("P1H"));
  }
}
