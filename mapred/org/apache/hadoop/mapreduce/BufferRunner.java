package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Map;

public class BufferRunner implements Runnable {
    private final boolean enableLogs;
    private final List<HadoopCLProfile> profiles;
    private final BufferManager<HadoopCLInputBuffer> freeInputBuffers;
    private final BufferManager<HadoopCLOutputBuffer> freeOutputBuffers; // exclusive
    private final KernelManager freeKernels; // exclusive

    private final HadoopCLLimitedQueue<HadoopCLInputBuffer> toRun;
    private final LinkedList<HadoopCLInputBuffer> toRunPrivate; // exclusive
    private final LinkedList<OutputBufferSoFar> toWrite; // exclusive

    // exclusive
    private final HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair> running;

    private final Class kernelClass;
    private final HadoopOpenCLContext clContext;

    public BufferRunner(Class kernelClass,
            BufferManager<HadoopCLInputBuffer> freeInputBuffers,
            BufferManager<HadoopCLOutputBuffer> freeOutputBuffers,
            KernelManager freeKernels,
            HadoopOpenCLContext clContext) {
        this.kernelClass = kernelClass;
        this.freeInputBuffers = freeInputBuffers;
        this.freeOutputBuffers = freeOutputBuffers;
        this.freeKernels = freeKernels;

        this.toRun = new HadoopCLLimitedQueue<HadoopCLInputBuffer>();
        this.toRunPrivate = new LinkedList<HadoopCLInputBuffer>();
        this.toWrite = new LinkedList<OutputBufferSoFar>();

        this.running = new HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair>();

        this.clContext = clContext;
        this.profiles = new LinkedList<HadoopCLProfile>();

        this.enableLogs = clContext.enableBufferRunnerDiagnostics();
    }

    public List<HadoopCLProfile> profiles() {
        return this.profiles;
    }

    private void log(String s) {
        if (enableLogs) {
            System.err.println(System.currentTimeMillis()+"|"+this.clContext.typeName()+" "+s);
        }
    }

    public void addWork(HadoopCLInputBuffer input) {
        // possible if getting DONE signal from main
        if (input != null) {
            input.clearNWrites();
        }
        log("Placing input buffer "+(input == null ? "null" : input.id)+" from main");
        this.toRun.add(input);
    }

    private HadoopCLKernel newKernelInstance() {
        AllocManager.TypeAlloc<HadoopCLKernel> result = freeKernels.nonBlockingAlloc();
        return (result == null ? null : result.obj());
    }

    private List<HadoopCLKernel> getCompleteKernels() {
        List<HadoopCLKernel> complete = new LinkedList<HadoopCLKernel>();
        for (HadoopCLKernel k : this.running.keySet()) {
            if (k.isComplete()) {
                complete.add(k);
            }
        }
        return complete;
    }

    private HadoopCLKernel getFirstCompleteKernel() {
        for (HadoopCLKernel k : this.running.keySet()) {
            if (k.isComplete()) {
                return k;
            }
        }
        return null;
    }

    private boolean startKernel(final HadoopCLKernel kernel,
            HadoopCLInputBuffer inputBuffer, HadoopCLOutputBuffer outputBuffer) {
        boolean success;

        kernel.tracker = inputBuffer.tracker.clone();
        outputBuffer.tracker = inputBuffer.tracker.clone();
        kernel.fill(inputBuffer, outputBuffer);
        try {
            OpenCLDriver.logger.log("launching kernel "+
                kernel.tracker.toString()+" on "+inputBuffer.tracker.toString()+
                "->"+outputBuffer.tracker.toString(), this.clContext);
            success = kernel.launchKernel();
            // new Thread(new Runnable() {
            //   @Override
            //   public void run() {
            //     kernel.waitForCompletion();
            //   }
            // }).start();
        } catch(Exception io) {
            throw new RuntimeException(io);
        }
        if (success) {
            inputBuffer.getProfile().startKernel();
            running.put(kernel, new HadoopCLInputOutputBufferPair(inputBuffer, outputBuffer, kernel));
        }
        return success;
    }

    private OutputBufferSoFar handleOutputBuffer(OutputBufferSoFar soFar) {
        try {
            soFar.buffer().getProfile().startWrite(soFar.buffer());
            int newProgress = soFar.buffer().putOutputsIntoHadoop(
                    this.clContext.getContext(), soFar.soFar());
            soFar.buffer().getProfile().stopWrite(soFar.buffer());
            if (newProgress == -1) {
                log("    Done writing "+soFar.buffer().id+", releasing");
                this.freeOutputBuffers.free(soFar.buffer());
                return null;
            } else {
                log("    Unable to complete output buffer, putting "+soFar.buffer().id+" back in toWrite with "+soFar.soFar()+" so far");
                soFar.setSoFar(newProgress);
                return soFar;
            }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private HadoopCLOutputBuffer allocOutputBufferWithInit(int outputPairsPerInput) {
        HadoopCLOutputBuffer result = null;
        BufferManager.TypeAlloc<HadoopCLOutputBuffer> outputBufferContainer =
            freeOutputBuffers.nonBlockingAlloc();
        if (outputBufferContainer != null) {
          result = outputBufferContainer.obj();
          if (outputBufferContainer.isFresh()) {
            result.initBeforeKernel(outputPairsPerInput, this.clContext);
          }
        }
        return result;
    }

    @Override
    public void run() {
        boolean mainDone = false;
        boolean forwardProgress = true;
        log("Entering BufferRunner");

        /*
         * I removed the condition !toWrite.isEmpty() because I'd rather
         * exit the loop and then just loop on toWrite after setting
         * usingOpencl to false (this should be more efficient so we're
         * not just constantly throwing exceptions
         */
        while (!mainDone || !running.isEmpty() ||
                !toRunPrivate.isEmpty()) {

            /*
             * Output Buffer Handling
             */
            OutputBufferSoFar soFar = toWrite.poll();
            if (soFar != null) {
                log("    Got output buffer "+soFar.buffer().id+" to write");
                log("      # output buffers available = "+this.freeOutputBuffers.nAvailable());
                int previously = soFar.soFar();
                OutputBufferSoFar cont = handleOutputBuffer(soFar);
                if (cont != null) {
                    if (cont.soFar() > previously) forwardProgress = true;
                    this.toWrite.add(cont);

                    log("      forwardProgress="+forwardProgress+" previously="+previously+" soFar="+cont.soFar()+" toWrite.size()="+this.toWrite.size());
                    boolean successfulUnlock = false;
                    try {
                        if (!forwardProgress) {
                            OpenCLDriver.resourcesLock.lock();
                            OpenCLDriver.spillLock.unlock();
                            successfulUnlock = true;
                            OpenCLDriver.logger.log("      Blocking on spillDone", this.clContext);
                            OpenCLDriver.resourcesAvailable.await();
                            OpenCLDriver.logger.log("      Unblocking on spillDone", this.clContext);
                        }
                    } catch (InterruptedException ie) {
                    } finally {
                        if (!successfulUnlock) {
                            OpenCLDriver.spillLock.unlock();
                        } else {
                          OpenCLDriver.resourcesLock.unlock();
                        }
                    }
                } else {
                  // Try to write the next available output buffer immediately
                  // because this means the SpillThread has free space
                  continue;
                  // forwardProgress = true;
                }
            } else {
              log("    No output buffers eligible for writing");
            }

            forwardProgress = false;

            /*
             * Kernel Completion Handling
             */
            HadoopCLKernel complete = getFirstCompleteKernel();
            // List<HadoopCLKernel> completed = getCompleteKernels();
            // if (!completed.isEmpty()) {
            //     log("  Detected "+completed.size()+" completed kernels, out of "+running.size()+" running");
            if (complete != null) {
                forwardProgress = true;
                // Try to either re-run incomplete kernels, or just
                // set the output buffers up for dumping
                // for (HadoopCLKernel k : completed) {
                    HadoopCLKernel k = complete;
                    HadoopCLInputOutputBufferPair pair = running.remove(k);
                    HadoopCLInputBuffer input = pair.inputBuffer();
                    HadoopCLOutputBuffer output = pair.outputBuffer();

                    OpenCLDriver.logger.log("recovering completed kernel "+
                        k.tracker.toString()+" for "+input.tracker.toString()+
                        "->"+output.tracker.toString(), this.clContext);

                    try {
                      pair.wrapperThread().join();
                    } catch(InterruptedException ie) {
                      throw new RuntimeException(ie);
                    }

                    int errCode = k.waitFor();
                    input.getProfile().stopKernel();

                    output.copyOverFromInput(input);
                    output.constructIterSet();
                    log("    Adding "+output.id+" to output buffers to write");
                    toWrite.add(new OutputBufferSoFar(output, 0));
                    boolean completedAll = input.completedAll();
                    if (input.completedAll()) {
                        log("    Input buffer "+input.id+" completed all work, releasing it and kernel "+k.id);
                        profiles.add(input.getProfile());
                        freeInputBuffers.free(input);
                        freeKernels.free(k);
                    } else {
                        input.tracker.incrementAttempt();
                        log("    Input buffer "+input.id+" has not finished all work");
                        HadoopCLOutputBuffer outputBuffer = allocOutputBufferWithInit(k.getOutputPairsPerInput());
                        if (outputBuffer != null) {
                            log("      Successfully allocated output buffer "+outputBuffer.id+" for input "+input.id);
                            if (!startKernel(k, input, outputBuffer)) {
                                log("      Failed to start kernel "+k.id+" on "+input.id+" -> "+outputBuffer.id);
                                toRunPrivate.add(input);
                                freeOutputBuffers.free(outputBuffer);
                                freeKernels.free(k);
                            } else {
                                log("      Successfully launched kernel "+k.id+" on "+input.id+" -> "+outputBuffer.id);
                            }
                        } else {
                            log("      Failed allocating an output buffer for "+input.id+", releasing input "+input.id+" and kernel "+k.id);
                            toRunPrivate.add(input);
                            freeKernels.free(k);
                        }
                    }
                // }
            }

            /*
             * Input Buffer Handling
             */
            BufferTypeContainer<HadoopCLInputBuffer> inputBufferContainer = 
                toRun.nonBlockingGet();
            // Special test for DONE signal from main thread
            if (inputBufferContainer != null && inputBufferContainer.get() == null) {
                log("   Got DONE signal from main");
                mainDone = true;
                continue;
            }

            HadoopCLInputBuffer inputBuffer = null; 
            if (inputBufferContainer == null) {
                // try again for any retry input buffers
                if (!toRunPrivate.isEmpty()) {
                    inputBuffer = toRunPrivate.poll();
                }
                if (inputBuffer != null) {
                    log("  Got input buffer "+inputBuffer.id+" from retry list");
                }
            } else {
                inputBuffer = inputBufferContainer.get();
                log("  Got input buffer "+inputBuffer.id+" from main");
            }


            if (inputBuffer != null) {
                // Have a kernel from main to run
                HadoopCLKernel k = null;
                HadoopCLOutputBuffer outputBuffer = null;
                if ((k = newKernelInstance()) != null &&
                        (outputBuffer = allocOutputBufferWithInit(k.getOutputPairsPerInput())) != null) {

                    log("    Allocated output buffer "+outputBuffer.id+", kernel "+k.id+" for processing of input buffer "+inputBuffer.id);
                    
                    if (!startKernel(k, inputBuffer, outputBuffer)) {
                        log("    Failed to start kernel, marking input "+inputBuffer.id+" to retry and freeing output "+outputBuffer.id+", kernel "+k.id);
                        toRunPrivate.add(inputBuffer);
                        freeOutputBuffers.free(outputBuffer);
                        freeKernels.free(k);
                    } else {
                        forwardProgress = true;
                        log("    Successfully started kernel "+k.id+" on "+inputBuffer.id+" -> "+outputBuffer.id);
                    }
                    log("    Continuing to next iteration");
                    continue; // one operation per iteration
                } else {
                    log("    Failed to allocate "+(k == null ? "kernel" : "output buffer")+", marking "+inputBuffer.id+" for retry");
                    toRunPrivate.add(inputBuffer);
                    if (outputBuffer != null) freeOutputBuffers.free(outputBuffer);
                    if (k != null) freeKernels.free(k);
                }
            } else {
              log("    No input buffer available to process");
            }
        }

        log("    At end, "+toWrite.size()+" output buffers remaining to write");
        if (!toWrite.isEmpty()) {
            // Little bit of work left, just finish off the remaining output
            // buffers
            
            // We're still using OpenCL, but we only have
            // output buffers left to process so we might as
            // well block
            // this.clContext.getContext().setUsingOpenCL(false);
            while (!toWrite.isEmpty()) {
                OutputBufferSoFar soFar = toWrite.poll();
                do {
                    int previously = soFar.soFar();
                    soFar = handleOutputBuffer(soFar);
                    if (soFar != null) {
                      forwardProgress = soFar.soFar() > previously;
                      log("      forwardProgress="+forwardProgress+" previously="+previously+" soFar="+soFar.soFar()+" toWrite.size()="+this.toWrite.size());
                      boolean successfulUnlock = false;
                      try {
                        if (!forwardProgress) {
                          OpenCLDriver.resourcesLock.lock();
                          OpenCLDriver.spillLock.unlock();
                          successfulUnlock = true;
                          OpenCLDriver.logger.log("      Blocking on spillDone", this.clContext);
                          OpenCLDriver.resourcesAvailable.await();
                          OpenCLDriver.logger.log("      Unblocking on spillDone", this.clContext);
                        }
                      } catch (InterruptedException ie) {
                      } finally {
                        if (!successfulUnlock) {
                          OpenCLDriver.spillLock.unlock();
                        } else {
                          OpenCLDriver.resourcesLock.unlock();
                        }
                      }
                    }
                } while(soFar != null);
            }
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("BufferRunner:\n");
        sb.append("  freeInputs: ");
        sb.append(this.freeInputBuffers.toString());
        sb.append("\n");
        sb.append("  freeOutputs: ");
        sb.append(this.freeOutputBuffers.toString());
        sb.append("\n");
        sb.append("  toRun: ");
        sb.append(this.toRun.toString());
        sb.append("\n");
        sb.append("  toRunPrivate: [ ");
        for (HadoopCLInputBuffer b : this.toRunPrivate) {
            sb.append(b.id);
            sb.append(" ");
        }
        sb.append("]\n");
        sb.append("  toWrite: [ ");
        for (OutputBufferSoFar b : this.toWrite) {
            sb.append(b.buffer().id);
            sb.append(" ");
        }
        sb.append("]\n");
        sb.append("  freeKernels: ");
        sb.append(this.freeKernels.toString());
        sb.append("\n");
        sb.append("  running: [ ");
        for (Map.Entry<HadoopCLKernel, HadoopCLInputOutputBufferPair> entry : running.entrySet()) {
            sb.append(entry.getValue().inputBuffer().id);
            sb.append("->");
            sb.append(entry.getKey().id);
            sb.append("->");
            sb.append(entry.getValue().outputBuffer().id);
            sb.append(" ");
        }
        sb.append("]");
        return sb.toString();
    }

    class OutputBufferSoFar {
        private int soFar;
        private final HadoopCLOutputBuffer buffer;

        public OutputBufferSoFar(HadoopCLOutputBuffer buffer, int setSoFar) {
            this.buffer = buffer;
            this.soFar = setSoFar;
        }
        public HadoopCLOutputBuffer buffer() { return this.buffer; }
        public int soFar() { return this.soFar; }
        public void setSoFar(int set) { this.soFar = set; }
    }
}
