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
package io.streamnative.pulsar.recipes.task;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskWorkerTest {
  private static final long TIMEOUT = 10L;
  @Mock private ExecutorService executor;
  @Mock private AutoCloseable closeable1;
  @Mock private AutoCloseable closeable2;
  private final List<AutoCloseable> closeables = new ArrayList<>();
  private TaskWorker taskWorker;

  @BeforeEach
  void beforeEach() {
    closeables.add(closeable1);
    closeables.add(closeable2);
    taskWorker = new TaskWorker(executor, closeables, TIMEOUT);
  }

  @Test
  void executorNotShutdownWithinTimeout() throws Exception {
    when(executor.awaitTermination(TIMEOUT, MILLISECONDS)).thenReturn(false);

    taskWorker.close();

    InOrder inOrder = Mockito.inOrder(executor, closeable1, closeable2);
    inOrder.verify(executor).shutdown();
    inOrder.verify(executor).awaitTermination(TIMEOUT, MILLISECONDS);
    inOrder.verify(executor).shutdownNow();
    inOrder.verify(closeable1).close();
    inOrder.verify(closeable2).close();
  }

  @Test
  void executorShutdownWithinTimeout() throws Exception {
    when(executor.awaitTermination(TIMEOUT, MILLISECONDS)).thenReturn(true);

    taskWorker.close();

    InOrder inOrder = Mockito.inOrder(executor, closeable1, closeable2);
    inOrder.verify(executor).shutdown();
    inOrder.verify(executor).awaitTermination(TIMEOUT, MILLISECONDS);
    verify(executor, never()).shutdownNow();
    inOrder.verify(closeable1).close();
    inOrder.verify(closeable2).close();
  }

  @Test
  void executorInterruptedDuringShutdown() throws Exception {
    when(executor.awaitTermination(TIMEOUT, MILLISECONDS)).thenThrow(InterruptedException.class);

    taskWorker.close();

    InOrder inOrder = Mockito.inOrder(executor, closeable1, closeable2);
    inOrder.verify(executor).shutdown();
    inOrder.verify(executor).awaitTermination(TIMEOUT, MILLISECONDS);
    inOrder.verify(executor).shutdownNow();
    inOrder.verify(closeable1).close();
    inOrder.verify(closeable2).close();
  }

  @Test
  void allCloseablesClosedDespiteExceptions() throws Exception {
    when(executor.awaitTermination(TIMEOUT, MILLISECONDS)).thenReturn(true);
    doThrow(Exception.class).when(closeable1).close();

    taskWorker.close();

    InOrder inOrder = Mockito.inOrder(executor, closeable1, closeable2);
    inOrder.verify(executor).shutdown();
    inOrder.verify(executor).awaitTermination(TIMEOUT, MILLISECONDS);
    verify(executor, never()).shutdownNow();
    inOrder.verify(closeable1).close();
    inOrder.verify(closeable2).close();
  }
}
