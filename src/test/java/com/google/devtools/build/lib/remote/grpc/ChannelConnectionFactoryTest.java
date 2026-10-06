// Copyright 2026 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.remote.grpc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.devtools.build.lib.remote.grpc.ChannelConnectionFactory.ChannelConnection;
import io.grpc.ManagedChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Tests for {@link ChannelConnectionFactory}. */
@RunWith(JUnit4.class)
public class ChannelConnectionFactoryTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private ManagedChannel channel;

  @After
  public void clearInterruptedStatus() {
    Thread.interrupted();
  }

  @Test
  public void close_uninterrupted_shutsDownChannel() throws Exception {
    when(channel.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);

    new ChannelConnection(channel).close();

    verify(channel).shutdownNow();
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }

  @Test
  public void close_alreadyInterrupted_waitsAndRestoresInterrupt() throws Exception {
    when(channel.awaitTermination(anyLong(), any(TimeUnit.class)))
        .thenAnswer(
            invocation -> {
              assertThat(Thread.currentThread().isInterrupted()).isFalse();
              return true;
            });
    Thread.currentThread().interrupt();

    new ChannelConnection(channel).close();

    verify(channel).shutdownNow();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
  }

  @Test
  public void close_interruptedWhileWaiting_finishesShutdownAndRestoresInterrupt()
      throws Exception {
    AtomicBoolean terminated = new AtomicBoolean();
    when(channel.awaitTermination(anyLong(), any(TimeUnit.class)))
        .thenThrow(new InterruptedException())
        .thenAnswer(
            invocation -> {
              terminated.set(true);
              return true;
            });

    new ChannelConnection(channel).close();

    verify(channel).shutdownNow();
    assertThat(terminated.get()).isTrue();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
  }

  @Test
  public void close_repeatedlyInterrupted_finishesShutdownAndRestoresInterrupt() throws Exception {
    AtomicBoolean terminated = new AtomicBoolean();
    when(channel.awaitTermination(anyLong(), any(TimeUnit.class)))
        .thenThrow(new InterruptedException())
        .thenThrow(new InterruptedException())
        .thenAnswer(
            invocation -> {
              terminated.set(true);
              return true;
            });

    new ChannelConnection(channel).close();

    assertThat(terminated.get()).isTrue();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
  }
}
