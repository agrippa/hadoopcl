package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Map;

public class BufferRunner implements Runnable {
    private static final boolean enableLogs = false;
    private final List<HadoopCLBuffer.Profile> profiles;
    private final BufferManager<HadoopCLInputBuffer> freeInputBuffers;
    private final BufferManager<HadoopCLOutputBuffer> freeOutputBuffers; // exclusive
    private final KernelManager freeKernels; // exclusive

    private final HadoopCLLimitedQueue<HadoopCLInputBuffer> toRun;
    private final LinkedList<HadoopCLInputBuffer> toRunPrivate; // exclusive
    private final LinkedList<OutputBufferSoFar> toWrite; // exclusive

    private final ReentrantLock spillLock;

    // exclusive
    private final HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair> running;

    private final Class kernelClass;
    private final HadoopOpenCLContext clContext;

    public BufferRunner(Class kernelClass,
            BufferManager<HadoopCLInputBuffer> freeInputBuffers,
            BufferManager<HadoopCLOutputBuffer> freeOutputBuffers,
            KernelManager freeKernels,
            HadoopOpenCLContext clContext, ReentrantLock spillLock) {
        this.kernelClass = kernelClass;
        this.freeInputBuffers = freeInputBuffers;
        this.freeOutputBuffers = freeOutputBuffers;
        this.freeKernels = freeKernels;

        this.toRun = new HadoopCLLimitedQueue<HadoopCLInputBuffer>();
        this.toRunPrivate = new LinkedList<HadoopCLInputBuffer>();
        this.toWrite = new LinkedList<OutputBufferSoFar>();

        this.running = new HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair>();

        this.clContext = clContext;
        this.profiles = new LinkedList<HadoopCLBuffer.Profile>();
        this.spillLock = spillLock;
    }

    public List<HadoopCLBuffer.Profile> profiles() {
        return this.profiles;
    }

    public void addWork(HadoopCLInputBuffer input) {
        // possible if getting DONE signal from main
        if (input != null) {
            input.clearNWrites();
        }
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

    private boolean startKernel(HadoopCLKernel kernel,
            HadoopCLInputBuffer inputBuffer, HadoopCLOutputBuffer outputBuffer) {
        boolean success;
        kernel.fill(inputBuffer, outputBuffer);
        try {
            success = kernel.launchKernel();
        } catch(Exception io) {
            throw new RuntimeException(io);
        }
        if (success) {
            inputBuffer.getProfile().startKernel();
            running.put(kernel, new HadoopCLInputOutputBufferPair(inputBuffer, outputBuffer));
        }
        return success;
    }

    private void log(String s) {
        System.err.println(System.currentTimeMillis()+" "+s);
    }

    private OutputBufferSoFar handleOutputBuffer(OutputBufferSoFar soFar) {
        try {
            soFar.buffer().getProfile().startWrite();
            int newProgress = soFar.buffer().putOutputsIntoHadoop(
                    this.clContext.getContext(), this.spillLock, soFar.soFar());
            soFar.buffer().getProfile().stopWrite();
            if (newProgress == -1) {
                if (enableLogs) {
                    log("    Done writing "+soFar.buffer().id+", releasing");
                }
                this.freeOutputBuffers.free(soFar.buffer());
                return null;
            } else {
                if (enableLogs) {
                    log("    Unable to complete output buffer, putting "+soFar.buffer().id+" back in toWrite");
                }
                soFar.setSoFar(newProgress);
                return soFar;
            }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    @Override
    public void run() {
        boolean mainDone = false;
        if (enableLogs) {
            log("Entering BufferRunner");
        }

        /*
         * I removed the condition !toWrite.isEmpty() because I'd rather
         * exit the loop and then just loop on toWrite after setting
         * usingOpencl to false (this should be more efficient so we're
         * not just constantly throwing exceptions
         */
        while (!mainDone || !running.isEmpty() ||
                !toRunPrivate.isEmpty()) {

            BufferTypeContainer<HadoopCLInputBuffer> inputBufferContainer = 
                toRun.nonBlockingGet();
            // Special test for DONE signal from main thread
            if (inputBufferContainer != null && inputBufferContainer.get() == null) {
                if (enableLogs) {
                    log("   Got DONE signal from main");
                }
                mainDone = true;
                continue;
            }

            HadoopCLInputBuffer inputBuffer = null; 
            if (inputBufferContainer == null) {
                // try again for any retry input buffers
                if (!toRunPrivate.isEmpty()) {
                    inputBuffer = toRunPrivate.poll();
                }
                if (inputBuffer != null && enableLogs) {
                    log("  Got input buffer "+inputBuffer.id+" from retry list");
                }
            } else {
                inputBuffer = inputBufferContainer.get();
                if (enableLogs) {
                    log("  Got input buffer "+inputBuffer.id+" from main");
                }
            }


            if (inputBuffer != null) {
                // Have a kernel from main to run
                HadoopCLKernel k = null;
                BufferManager.TypeAlloc<HadoopCLOutputBuffer> outputBufferContainer = null;
                if ((k = newKernelInstance()) != null &&
                        (outputBufferContainer = freeOutputBuffers.nonBlockingAlloc()) != null) {

                    HadoopCLOutputBuffer outputBuffer = outputBufferContainer.obj();
                    if (enableLogs) {
                        log("    Allocated output buffer "+outputBuffer.id+", kernel "+k.id+" for processing of input buffer "+inputBuffer.id);
                    }
                    if (outputBufferContainer.isFresh()) {
                        if (enableLogs) {
                            log("      Initializing fresh output buffer "+outputBuffer.id);
                        }
                        outputBuffer.initBeforeKernel(
                                k.getOutputPairsPerInput(), this.clContext);
                    }
                    if (!startKernel(k, inputBuffer, outputBuffer)) {
                        if (enableLogs) {
                            log("    Failed to start kernel, marking input "+inputBuffer.id+" to retry and freeing output "+outputBuffer.id+", kernel "+k.id);
                        }
                        toRunPrivate.add(inputBuffer);
                        freeOutputBuffers.free(outputBuffer);
                        freeKernels.free(k);
                    } else {
                        if (enableLogs) {
                            log("    Successfully started kernel on "+inputBuffer.id+" -> "+outputBuffer.id);
                        }
                    }
                    if (enableLogs) {
                        log("    Continuing to next iteration");
                    }
                    continue; // one operation per iteration
                } else {
                    if (enableLogs) {
                        log("    Failed to allocate "+(k == null ? "kernel" : "output buffer")+", marking "+inputBuffer.id+" for retry");
                    }
                    toRunPrivate.add(inputBuffer);
                    if (outputBufferContainer != null) {
                        if (outputBufferContainer.isFresh()) {
                            // k must be non-null here because it is allocated first in an AND statement
                            outputBufferContainer.obj().initBeforeKernel(k.getOutputPairsPerInput(), this.clContext);
                        }
                        freeOutputBuffers.free(outputBufferContainer.obj());
                    }
                    if (k != null) freeKernels.free(k);
                }
            }

            List<HadoopCLKernel> completed = getCompleteKernels();
            if (completed.size() > 0 && enableLogs) {
                log("  Detected "+completed.size()+" completed kernels, out of "+running.size()+" running");
            }
            if (!completed.isEmpty()) {
                // Try to either re-run incomplete kernels, or just
                // set the output buffers up for dumping
                boolean doContinue = false; // we only want to continue if we actually made progress on this iter
                for (HadoopCLKernel k : completed) {
                    HadoopCLInputOutputBufferPair pair = running.remove(k);
                    HadoopCLInputBuffer input = pair.inputBuffer();
                    HadoopCLOutputBuffer output = pair.outputBuffer();

                    int errCode = k.waitFor();
                    input.getProfile().stopKernel();

                    System.out.println("Output tried to allocate "+((IntFsvecHadoopCLOutputMapperBuffer)output).memAuxIntIncr[0]+
                        " but had limit "+((IntFsvecHadoopCLOutputMapperBuffer)output).outputValIndices.length);

                    output.copyOverFromInput(input);
                    if (enableLogs) {
                        log("    Adding "+output.id+" to output buffers to write");
                    }
                    toWrite.add(new OutputBufferSoFar(output, 0));
                    boolean completedAll = input.completedAll();
                    if (input.completedAll()) {
                        if (enableLogs) {
                            log("    Input buffer "+input.id+" completed all work, releasing it and kernel "+k.id);
                        }
                        profiles.add(input.getProfile());
                        freeInputBuffers.free(input);
                        freeKernels.free(k);
                    } else {
                        if (enableLogs) {
                            log("    Input buffer "+input.id+" has not finished all work");
                        }
                        BufferManager.TypeAlloc<HadoopCLOutputBuffer> outputBufferContainer =
                            freeOutputBuffers.nonBlockingAlloc();
                        if (outputBufferContainer != null) {
                            HadoopCLOutputBuffer outputBuffer = outputBufferContainer.obj();
                            if (enableLogs) {
                                log("      Successfully allocated output buffer "+outputBuffer.id+" for input "+input.id);
                            }
                            if (outputBufferContainer.isFresh()) {
                                outputBuffer.initBeforeKernel(k.getOutputPairsPerInput(),
                                        this.clContext);
                            }
                            if (!startKernel(k, input, outputBuffer)) {
                                if (enableLogs) {
                                    log("      Failed to start kernel "+k.id+" on "+input.id+" -> "+outputBuffer.id);
                                }
                                toRunPrivate.add(input);
                                freeOutputBuffers.free(outputBuffer);
                                freeKernels.free(k);
                                // Continue if we've just created work early in
                                //   the pipeline.
                                // In this case, we've created work because we
                                //   failed to launch a kernel.
                                // This usually happens because we're out of GPU
                                //   memory, so continuing may not be the right
                                //   thing here.
                                doContinue = true; // only continue if we've just created work early in the pipeline
                            } else {
                                if (enableLogs) {
                                    log("      Successfully launched kernel "+k.id+" on "+input.id+" -> "+outputBuffer.id);
                                }
                            }
                        } else {
                            if (enableLogs) {
                                log("      Failed allocating an output buffer for "+input.id+", releasing input "+input.id+" and kernel "+k.id);
                            }
                            toRunPrivate.add(input);
                            freeKernels.free(k);
                        }
                    }
                }
                // if (doContinue) 
                {
                    if (enableLogs) {
                        log("    Continuing to next iteration");
                    }
                    continue;
                }
            }

            OutputBufferSoFar soFar = toWrite.poll();
            if (soFar != null) {
                if (enableLogs) {
                    log("    Got output buffer "+soFar.buffer().id+" to write");
                }
                OutputBufferSoFar cont = handleOutputBuffer(soFar);
                if (cont != null) {
                    this.toWrite.add(cont);
                }
            }
        }

        if (!toWrite.isEmpty()) {
            // Little bit of work left, just finish off the remaining output
            // buffers
            
            // We're still using OpenCL, but we only have
            // output buffers left to process so we might as
            // well block
            this.clContext.getContext().setUsingOpenCL(false);
            while (!toWrite.isEmpty()) {
                OutputBufferSoFar soFar = toWrite.poll();
                do {
                    soFar = handleOutputBuffer(soFar);
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
