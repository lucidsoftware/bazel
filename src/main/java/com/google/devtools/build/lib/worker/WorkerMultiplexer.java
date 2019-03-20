// Copyright 2018 The Bazel Authors. All rights reserved.
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

package com.google.devtools.build.lib.worker;

import com.google.devtools.build.lib.shell.Subprocess;
import com.google.devtools.build.lib.shell.SubprocessBuilder;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.devtools.build.lib.vfs.Path;
import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * An intermediate worker that sends request and receives response from the
 * worker processes.
 */
public class WorkerMultiplexer extends Thread {
  /**
   * There should only be one WorkerMultiplexer corresponding to workers with
   * the same mnemonic. If the WorkerMultiplexer has been constructed, other
   * workers should point to the same one. The hash of WorkerKey is used as
   * key.
   */
  private static Map<Integer, WorkerMultiplexer> multiplexerInstance = new HashMap<>();
  /**
   * An accumulator of how many WorkerProxies are referencing a particular
   * WorkerMultiplexer.
   */
  private static Map<Integer, Integer> multiplexerRefCount = new HashMap<>();
  /**
   * A semaphore to protect multiplexerInstance and multiplexerRefCount objects.
   */
  private static Semaphore semMultiplexer = new Semaphore(1);
  /**
   * WorkerMultiplexer is running as a thread on its own. When worker process
   * returns the WorkResponse, it is stored in this map and wait for
   * WorkerProxy to retrieve the response.
   */
  private Map<Integer, InputStream> workerProcessResponse;
  /**
   * A semaphore to protect workerProcessResponse object.
   */
  private Semaphore semWorkerProcessResponse;
  /**
   * After sending the WorkRequest, WorkerProxy will wait on a semaphore to be
   * released. WorkerMultiplexer is responsible to release the corresponding
   * semaphore in order to signal WorkerProxy that the WorkerResponse has been
   * received.
   */
  private Map<Integer, Semaphore> responseChecker;
  /**
   * A semaphore to protect responseChecker object.
   */
  private Semaphore semResponseChecker;
  /**
   * The worker process that this WorkerMultiplexer should be talking to.
   */
  private Subprocess process;
  /**
   * A semaphore to protect process object.
   */
  private Semaphore semAccessProcess;

  private Thread shutdownHook;
  private Integer workerHash;

  WorkerMultiplexer(Integer workerHash) {
    semAccessProcess = new Semaphore(1);
    semWorkerProcessResponse = new Semaphore(1);
    semResponseChecker = new Semaphore(1);
    responseChecker = new HashMap<>();
    workerProcessResponse = new HashMap<>();
    this.workerHash = workerHash;

    final WorkerMultiplexer self = this;
    this.shutdownHook =
      new Thread(
        () -> {
          try {
            self.shutdownHook = null;
            self.destroyMultiplexer();
          } finally {
            // We can't do anything here.
          }
        });
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  /**
   * Returns a WorkerMultiplexer instance to WorkerProxy. WorkerProxys with the
   * same workerHash talk to the same WorkerMultiplexer.
   */
  public synchronized static WorkerMultiplexer getInstance(Integer workerHash) {
    try {
      semMultiplexer.acquire();
      if (!multiplexerInstance.containsKey(workerHash)) {
        multiplexerInstance.put(workerHash, new WorkerMultiplexer(workerHash));
        multiplexerRefCount.put(workerHash, 0);
      }
      multiplexerRefCount.put(workerHash, multiplexerRefCount.get(workerHash) + 1);
      return multiplexerInstance.get(workerHash);
    } catch (InterruptedException e) {
      e.printStackTrace();
      return null;
    } finally {
      semMultiplexer.release();
    }
  }

  public synchronized static Integer getRefCount(Integer workerHash) {
    try {
      semMultiplexer.acquire();
      return multiplexerRefCount.get(workerHash);
    } catch (InterruptedException e) {
      e.printStackTrace();
      return null;
    } finally {
      semMultiplexer.release();
    }
  }

  public synchronized static void decreaseRefCount(Integer workerHash) {
    try {
      semMultiplexer.acquire();
      multiplexerRefCount.put(workerHash, multiplexerRefCount.get(workerHash) - 1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      semMultiplexer.release();
    }
  }

  /**
   * Only start one worker process for each WorkerMultiplexer, if it hasn't.
   */
  public synchronized void createProcess(WorkerKey workerKey, Path workDir, Path logFile) throws IOException {
    try {
      semAccessProcess.acquire();
      if (this.process == null || this.process.finished()) {
        List<String> args = workerKey.getArgs();
        File executable = new File(args.get(0));
        if (!executable.isAbsolute() && executable.getParent() != null) {
          args = new ArrayList<>(args);
          args.set(0, new File(workDir.getPathFile(), args.get(0)).getAbsolutePath());
        }
        SubprocessBuilder processBuilder = new SubprocessBuilder();
        processBuilder.setArgv(args);
        processBuilder.setWorkingDirectory(workDir.getPathFile());
        processBuilder.setStderr(logFile.getPathFile());
        processBuilder.setEnv(workerKey.getEnv());
        this.process = processBuilder.start();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      semAccessProcess.release();
    }
    if (!this.isAlive()) {
      this.start();
    }
  }

  synchronized void destroyMultiplexer() {
    if (shutdownHook != null) {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }
    try {
      semMultiplexer.acquire();
      multiplexerInstance.remove(workerHash);
      multiplexerRefCount.remove(workerHash);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      semMultiplexer.release();
    }
    try {
      semAccessProcess.acquire();
      if (this.process != null) {
        destroyProcess(this.process);
      }
      semAccessProcess.release();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void destroyProcess(Subprocess process) {
    boolean wasInterrupted = false;
    try {
      process.destroy();
      while (true) {
        try {
          process.waitFor();
          return;
        } catch (InterruptedException ie) {
          wasInterrupted = true;
        }
      }
    } finally {
      // Read this for detailed explanation: http://www.ibm.com/developerworks/library/j-jtp05236/
      if (wasInterrupted) {
        Thread.currentThread().interrupt(); // preserve interrupted status
      }
    }
  }

  public boolean isProcessAlive() {
    try {
      semAccessProcess.acquire();
      return !this.process.finished();
    } catch (InterruptedException e) {
      e.printStackTrace();
      return false;
    } finally {
      semAccessProcess.release();
    }
  }

  /**
   * Pass the WorkRequest to worker process.
   */
  public synchronized void putRequest(byte[] request) throws IOException {
    OutputStream stdin = process.getOutputStream();
    stdin.write(request);
    stdin.flush();
  }

  /**
   * A WorkerProxy waits on a semaphore for the WorkResponse returned from worker process.
   */
  public InputStream getResponse(int workerId) throws InterruptedException {
    Semaphore waitForResponse;
    try {
      semResponseChecker.acquire();
      waitForResponse = responseChecker.get(workerId);
    } catch (InterruptedException e) {
      throw e;
    } finally {
      semResponseChecker.release();
    }

    try {
      waitForResponse.acquire();
    } catch (InterruptedException e) {
      // Return empty InputStream if there is a compilation error.
      return new ByteArrayInputStream(new byte[0]);
    }

    try {
      semWorkerProcessResponse.acquire();
      InputStream response = workerProcessResponse.get(workerId);
      return response;
    } catch (InterruptedException e) {
      throw e;
    } finally {
      semWorkerProcessResponse.release();
    }
  }

  /**
   * Reset the map that indicates if the WorkResponses have been returned.
   */
  public void setResponseChecker(int workerId) throws InterruptedException {
    try {
      semResponseChecker.acquire();
      responseChecker.put(workerId, new Semaphore(0));
    } catch (InterruptedException e) {
      throw e;
    } finally {
      semResponseChecker.release();
    }
  }

  /**
   * When it gets a WorkResponse from worker process, put that WorkResponse in
   * workerProcessResponse and signal responseChecker.
   */
  public void waitRequest() throws InterruptedException, IOException {
    InputStream stdout = process.getInputStream();

    WorkResponse parsedResponse;
    try {
      parsedResponse = WorkResponse.parseDelimitedFrom(stdout);
    } catch (IOException e) {
      throw e;
    }

    if (parsedResponse == null) return;

    int workerId = parsedResponse.getRequestId();
    ByteArrayOutputStream tempOs = new ByteArrayOutputStream();
    parsedResponse.writeDelimitedTo(tempOs);

    try {
      semWorkerProcessResponse.acquire();
      workerProcessResponse.put(workerId, new ByteArrayInputStream(tempOs.toByteArray()));
    } catch (InterruptedException e) {
      throw e;
    } finally {
      semWorkerProcessResponse.release();
    }

    try {
      semResponseChecker.acquire();
      responseChecker.get(workerId).release();
    } catch (InterruptedException e) {
      throw e;
    } finally {
      semResponseChecker.release();
    }
  }

  /**
   * A multiplexer thread that listens to the WorkResponses from worker process.
   */
  public void run() {
    while (!this.interrupted()) {
      try {
        waitRequest();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
