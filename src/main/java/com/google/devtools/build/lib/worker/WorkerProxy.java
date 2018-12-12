// Copyright 2018 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.devtools.build.lib.worker;

import com.google.devtools.build.lib.sandbox.SandboxHelpers;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * A proxy that talks to the multiplexers
 */
final class WorkerProxy extends Worker {
  private WorkerMultiplexer workerMultiplexer;
  private ByteArrayOutputStream request;
  private Thread shutdownHook;

  WorkerProxy(WorkerKey workerKey, int workerId, Path workDir, Path logFile) {
    super(workerKey, workerId, workDir, logFile);
    this.workerMultiplexer = WorkerMultiplexer.getInstance(workerKey.hashCode());
    this.request = new ByteArrayOutputStream();

    final Worker self = this;
    this.shutdownHook =
      new Thread(
        () -> {
          try {
            self.shutdownHook = null;
            self.destroy();
          } catch (IOException e) {
            // We can't do anything here.
          }
        });
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  @Override
  void createProcess() throws IOException {
    workerMultiplexer.createProcess(workerKey, workDir, logFile);
  }

  @Override
  boolean isAlive() {
    // This is horrible, but Process.isAlive() is only available from Java 8 on and this is the
    // best we can do prior to that.
    return workerMultiplexer.isProcessAlive();
  }

  @Override
  public void prepareExecution(
          Map<PathFragment, Path> inputFiles, SandboxHelpers.SandboxOutputs outputs, Set<PathFragment> workerFiles)
          throws IOException {
      createProcess();
  }

  @Override
  synchronized void destroy() {
    if (shutdownHook != null) {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }
    if (workerMultiplexer.isAlive()) {
      workerMultiplexer.interrupt();
    }
    workerMultiplexer.destroyMultiplexer();
  }

  @Override
  InputStream getInputStream() {
    byte[] requestBytes = request.toByteArray();
    request.reset();
    try {
      workerMultiplexer.putRequest(requestBytes);
      workerMultiplexer.setResponseMap(workerId, requestBytes);
      return workerMultiplexer.getResponse(workerId, requestBytes);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  OutputStream getOutputStream() {
    return request;
  }
}