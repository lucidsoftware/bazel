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
 * An intermediate worker that receives response from the worker processes
 */
public class WorkerMultiplexer extends Thread {
    private static Semaphore semInstanceMap = new Semaphore(1);
    private static Map<Integer, WorkerMultiplexer> instanceMap = new HashMap<>();
    private Map<Integer, InputStream> responseMap;
    private Map<Integer, Semaphore> semResponseMapNotEmpty;
    private Semaphore semResponseMap;
    private Semaphore semResponseNotEmpty;
    private Semaphore semAccessProcess;
    private Semaphore toPrint;

    private Subprocess process;
    private Integer workerHash;

    private Thread shutdownHook;

    WorkerMultiplexer(Integer workerHash) {
        semAccessProcess = new Semaphore(1);
        semResponseMap = new Semaphore(1);
        semResponseNotEmpty = new Semaphore(1);
        toPrint = new Semaphore(1);
        semResponseMapNotEmpty = new HashMap<>();
        responseMap = new HashMap<>();
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

    public synchronized static WorkerMultiplexer getInstance(Integer workerHash) {
        try {
            semInstanceMap.acquire();
            if (!instanceMap.containsKey(workerHash)) {
                instanceMap.put(workerHash, new WorkerMultiplexer(workerHash));
            }
            WorkerMultiplexer receiver = instanceMap.get(workerHash);
            return receiver;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        } finally {
            semInstanceMap.release();
        }
    }

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
            semInstanceMap.acquire();
            instanceMap.remove(workerHash);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            semInstanceMap.release();
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

    public synchronized void putRequest(byte[] request) throws IOException {
        OutputStream stdin = process.getOutputStream();
        stdin.write(request);
        stdin.flush();
    }

    public InputStream getResponse(int workerId, byte[] request) throws Exception {
        Semaphore waitForResponse;
        try {
            semResponseNotEmpty.acquire();
            waitForResponse = semResponseMapNotEmpty.get(workerId);
        } catch (InterruptedException e) {
            throw e;
        } finally {
            semResponseNotEmpty.release();
        }
        try {
            toPrint.acquire();
            InputStream targetStream = new ByteArrayInputStream(request);
            System.out.println("WAIT ON ID: " + workerId);
            System.out.println(WorkerProtocol.WorkRequest.parseDelimitedFrom(targetStream).getInputsList().get(0));
            System.out.println(waitForResponse.toString());
        } catch (InterruptedException e) {
            throw e;
        } finally {
            toPrint.release();
        }
        waitForResponse.acquire();
        try {
            toPrint.acquire();
            InputStream targetStream = new ByteArrayInputStream(request);
            System.out.println("GET SEM ON ID: " + workerId);
            System.out.println(WorkerProtocol.WorkRequest.parseDelimitedFrom(targetStream).getInputsList().get(0));
        } catch (InterruptedException e) {
            throw e;
        } finally {
            toPrint.release();
        }
        try {
            semResponseMap.acquire();
            InputStream response = responseMap.get(workerId);
            return response;
        } catch (InterruptedException e) {
            throw e;
        } finally {
            semResponseMap.release();
        }
    }

    public void setResponseMap(int workerId, byte[] request) throws InterruptedException, IOException {
        try {
            semResponseNotEmpty.acquire();
            semResponseMapNotEmpty.put(workerId, new Semaphore(0));

            try {
                toPrint.acquire();
                InputStream targetStream = new ByteArrayInputStream(request);
                System.out.println("NEW SEM");
                Semaphore tempSem = semResponseMapNotEmpty.get(workerId);
                System.out.println(tempSem.toString());
                System.out.println(WorkerProtocol.WorkRequest.parseDelimitedFrom(targetStream).getInputsList().get(0));
            } catch (InterruptedException e) {
                throw e;
            } finally {
                toPrint.release();
            }

        } catch (InterruptedException e) {
            throw e;
        } finally {
            semResponseNotEmpty.release();
        }
    }

    public void waitRequest() throws InterruptedException, IOException {
        InputStream stdout = process.getInputStream();

        WorkResponse parsedResponse;
        try {
            parsedResponse = WorkResponse.parseDelimitedFrom(stdout);
        } catch (IOException e) {
            throw e;
        }

        try {
            toPrint.acquire();
            System.out.println("BACK FROM ANNEX");
            System.out.println(parsedResponse.getOutput());
        } catch (InterruptedException e) {
            throw e;
        } finally {
            toPrint.release();
        }

        if (parsedResponse == null) return;

        int workerId = parsedResponse.getRequestId();
        ByteArrayOutputStream tempOs = new ByteArrayOutputStream();
        parsedResponse.writeDelimitedTo(tempOs);

        try {
            semResponseMap.acquire();
            semResponseNotEmpty.acquire();
            responseMap.put(workerId, new ByteArrayInputStream(tempOs.toByteArray()));
            
            try {
                toPrint.acquire();
                System.out.println("HOW MANY SEM");
                Semaphore tempSem = semResponseMapNotEmpty.get(workerId);
                System.out.println(tempSem.toString());
                tempSem.release();
                System.out.println(parsedResponse.getOutput());
                System.out.println(tempSem.availablePermits());
            } catch (InterruptedException e) {
                throw e;
            } finally {
                toPrint.release();
            }

        } catch (InterruptedException e) {
            throw e;
        } finally {
            semResponseNotEmpty.release();
            semResponseMap.release();
        }
    }

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
