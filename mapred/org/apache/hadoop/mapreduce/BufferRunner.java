package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BufferRunner implements Runnable {
    private final boolean enableLogs;
    private final List<HadoopCLProfile> profiles;
    private final BufferManager<HadoopCLInputBuffer> freeInputBuffers;
    private final BufferManager<HadoopCLOutputBuffer> freeOutputBuffers; // exclusive
    private final KernelManager freeKernels; // exclusive

    private final HadoopCLLimitedQueue<HadoopCLInputBuffer> toRun;
    private final LinkedList<HadoopCLInputBuffer> toRunPrivate; // exclusive
    private final LinkedList<OutputBufferSoFar> toWrite; // exclusive
    // private final LinkedList<HadoopCLKernel> toCopyFromOpenCL; // exclusive
    private final ConcurrentLinkedQueue<HadoopCLKernel> toCopyFromOpenCL;

    public static final AtomicBoolean somethingHappened = new AtomicBoolean(false);
    public static final AtomicBoolean somethingHappenedCombiner = new AtomicBoolean(false);
    private final AtomicBoolean somethingHappenedLocal;
    private boolean mainDone;

    // exclusive
    // private final HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair> running;
    // private final List<HadoopCLKernel> running;
    private final AtomicInteger kernelsActive;

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
        // this.toCopyFromOpenCL = new LinkedList<HadoopCLKernel>();
        this.toCopyFromOpenCL = new ConcurrentLinkedQueue<HadoopCLKernel>();

        // this.running = new HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair>();
        // this.running = new LinkedList<HadoopCLKernel>();
        kernelsActive = new AtomicInteger();

        this.clContext = clContext;
        this.profiles = new LinkedList<HadoopCLProfile>();

        this.enableLogs = clContext.enableBufferRunnerDiagnostics();
        this.mainDone = false;

        if (this.clContext.isCombiner()) {
            this.somethingHappenedLocal = somethingHappenedCombiner;
        } else {
            this.somethingHappenedLocal = somethingHappened;
        }
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
        // LOG:DIAGNOSTIC
        // log("Placing input buffer "+(input == null ? "null" : input.id)+" from main");

        synchronized (this.somethingHappenedLocal) {
            this.toRun.add(input);
            this.somethingHappenedLocal.set(true);
            this.somethingHappenedLocal.notify();
        }
    }

    private HadoopCLKernel newKernelInstance() {
        AllocManager.TypeAlloc<HadoopCLKernel> result = freeKernels.nonBlockingAlloc();
        return (result == null ? null : result.obj());
    }

    // private List<HadoopCLKernel> getCompleteKernels() {
    //     List<HadoopCLKernel> complete = new LinkedList<HadoopCLKernel>();
    //     // for (HadoopCLKernel k : this.running.keySet()) {
    //     for (HadoopCLKernel k : this.running) {
    //         if (k.isComplete()) {
    //             complete.add(k);
    //         }
    //     }
    //     return complete;
    // }

    // private HadoopCLKernel getFirstCompleteKernel() {
    //     for (HadoopCLKernel k : this.running) {
    //         if (k.isComplete()) {
    //             return k;
    //         }
    //     }
    //     return null;
    // }

    private void spawnKernelTrackingThread(final HadoopCLKernel kernel) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                kernel.waitForCompletion();
                // LOG:DIAGNOSTIC
                // log("  Detected completed kernel "+kernel.id);
                // LOG:PROFILE
                // OpenCLDriver.logger.log("recovering completed kernel "+kernel.tracker.toString(), clContext);
                toCopyFromOpenCL.add(kernel);
                kernelsActive.getAndDecrement();
                synchronized (somethingHappenedLocal) {
                    kernel.openclProfile.stopKernel();
                    somethingHappenedLocal.set(true);
                    somethingHappenedLocal.notify();
                }
            }
        }).start();
    }

    private boolean startKernel(final HadoopCLKernel kernel,
            HadoopCLInputBuffer inputBuffer) {
        boolean success;

        kernel.tracker = inputBuffer.tracker.clone();
        // outputBuffer.tracker = inputBuffer.tracker.clone();
        kernel.fill(inputBuffer);
        try {
            // LOG:PROFILE
            // OpenCLDriver.logger.log("launching kernel "+kernel.tracker.toString(), this.clContext);
            // LOG:PROFILE
            // OpenCLDriver.logger.log("starting kernel", this.clContext);
            success = kernel.launchKernel();
            // LOG:PROFILE
            // OpenCLDriver.logger.log("returning from kernel start", this.clContext);
        } catch(Exception io) {
            throw new RuntimeException(io);
        }
        if (success) {
            kernelsActive.getAndIncrement();
            kernel.openclProfile = inputBuffer.getProfile();
            kernel.openclProfile.startKernel();
            spawnKernelTrackingThread(kernel);
            // running.put(kernel, new HadoopCLInputOutputBufferPair(kernel));
            // running.add(kernel);
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
                // LOG:DIAGNOSTIC
                // log("    Done writing "+soFar.buffer().id+", releasing");
                this.freeOutputBuffers.free(soFar.buffer());
                return null;
            } else {
                // LOG:DIAGNOSTIC
                // log("    Unable to complete output buffer, putting "+soFar.buffer().id+" back in toWrite with "+soFar.soFar()+" so far");
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

    private void handleOpenCLCopy(HadoopCLKernel complete,
            HadoopCLOutputBuffer output) {
        // LOG:PROFILE
        // OpenCLDriver.logger.log("started reading from opencl", this.clContext);
        complete.prepareForRead(output);
        complete.waitFor();

        output.copyOverFromKernel(complete);
        output.tracker = complete.tracker.clone();
        // LOG:PROFILE
        // OpenCLDriver.logger.log("done reading from opencl", this.clContext);

        // LOG:DIAGNOSTIC
        // log("    Adding "+output.id+" to output buffers to write");

        boolean completedAll = output.completedAll();
        toWrite.add(new OutputBufferSoFar(output, 0));

        if (!completedAll) {
            // LOG:DIAGNOSTIC
            // log("      Retrying kernel "+complete.id+" due to completedAll="+completedAll);
            complete.tracker.incrementAttempt();
            try {
                if (!complete.relaunchKernel()) {
                    throw new RuntimeException("Failure to re-launch kernel");
                }
            } catch (IOException io) {
                throw new RuntimeException(io);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            // LOG:PROFILE
            // OpenCLDriver.logger.log("launching kernel "+complete.tracker.toString(), this.clContext);
            complete.openclProfile.startKernel();
            kernelsActive.getAndIncrement();
            spawnKernelTrackingThread(complete);
            // running.put(complete, new HadoopCLInputOutputBufferPair(complete));
            // running.add(complete);
        } else {
            // LOG:DIAGNOSTIC
            // log("      Releasing kernel "+complete.id+" due to completedAll="+completedAll);
            freeKernels.free(complete);
        }
    }

    private boolean doOutputBuffers() {
        boolean forwardProgress = false;

        OutputBufferSoFar soFar = null;
        while ((soFar = toWrite.poll()) != null) {
            // LOG:DIAGNOSTIC
            // log("    Got output buffer "+soFar.buffer().id+" to write");

            int previously = soFar.soFar();
            OutputBufferSoFar cont = handleOutputBuffer(soFar);
            if (cont != null) {
                if (cont.soFar() > previously) {
                    forwardProgress = true;
                }
                this.toWrite.add(cont);
                break;
            } else {
                forwardProgress = true;
            }
        }

        return forwardProgress;
    }

    private boolean doKernelCopyBack() {
        boolean forwardProgress = false;

        do {
            HadoopCLKernel kernel = toCopyFromOpenCL.poll();
            if (kernel == null) break;

            HadoopCLOutputBuffer output =
                allocOutputBufferWithInit(kernel.getOutputPairsPerInput());
            if (output == null) {
                toCopyFromOpenCL.add(kernel);
                break;
            }

            forwardProgress = true;
            handleOpenCLCopy(kernel, output);
        } while (true);

        return forwardProgress;
    }

    // private boolean doKernelCompletion() {
    //     boolean forwardProgress = false;

    //     List<HadoopCLKernel> completed = getCompleteKernels();

    //     for (HadoopCLKernel complete : completed) {
    //         // Try to either re-run incomplete kernels, or just
    //         // set the output buffers up for dumping
    //         // final HadoopCLInputOutputBufferPair pair = running.remove(complete);
    //         running.remove(complete);
    //         forwardProgress = true;

    //         log("  Detected completed kernel "+complete.id);
    //         OpenCLDriver.logger.log("recovering completed kernel "+
    //             complete.tracker.toString(), this.clContext);

    //         complete.openclProfile.stopKernel();
    //         // try {
    //         //   pair.wrapperThread().join();
    //         // } catch(InterruptedException ie) {
    //         //   throw new RuntimeException(ie);
    //         // }

    //         HadoopCLOutputBuffer output =
    //             allocOutputBufferWithInit(complete.getOutputPairsPerInput());

    //         if (output == null) {
    //             toCopyFromOpenCL.add(complete);
    //         } else {
    //             handleOpenCLCopy(complete, output);
    //         }
    //     }

    //     return forwardProgress;
    // }

    private HadoopCLInputBuffer getInputBuffer() {

        HadoopCLInputBuffer inputBuffer = null;
        BufferTypeContainer<HadoopCLInputBuffer> inputBufferContainer = 
            toRun.nonBlockingGet();

        if (inputBufferContainer != null) {
            if (inputBufferContainer.get() == null) {
                // LOG:DIAGNOSTIC
                // log("   Got DONE signal from main");
                this.mainDone = true;
            } else {
                inputBuffer = inputBufferContainer.get();
                // LOG:DIAGNOSTIC
                // log("  Got input buffer "+inputBuffer.id+" from main");
            }
        }

        if (inputBuffer == null) {
            // try again for any retry input buffers
            if (!toRunPrivate.isEmpty()) {
                inputBuffer = toRunPrivate.poll();
            }
            if (inputBuffer != null) {
                // LOG:DIAGNOSTIC
                // log("  Got input buffer "+inputBuffer.id+" from retry list");
            }
        }

        return inputBuffer;
    }

    private boolean doInputBuffers() {
        boolean forwardProgress = false;

        HadoopCLInputBuffer inputBuffer;
        if ((inputBuffer = getInputBuffer()) != null) {
        // while ((inputBuffer = getInputBuffer()) != null) {
            HadoopCLKernel k = newKernelInstance();
            if (k != null) {
                // LOG:DIAGNOSTIC
                // log("    Allocated kernel "+k.id+" for processing of input buffer "+inputBuffer.id);
                
                if (!startKernel(k, inputBuffer)) {
                    // LOG:DIAGNOSTIC
                    // log("    Failed to start kernel, marking input "+inputBuffer.id+" to retry and freeing kernel "+k.id);
                    toRunPrivate.add(inputBuffer);
                    freeKernels.free(k);
                } else {
                    forwardProgress = true;
                    profiles.add(inputBuffer.getProfile());
                    freeInputBuffers.free(inputBuffer);
                    // LOG:DIAGNOSTIC
                    // log("    Successfully started kernel "+k.id+" on "+inputBuffer.id);
                }
            } else {
                // LOG:DIAGNOSTIC
                // log("    Failed to allocate kernel, marking "+inputBuffer.id+" for retry");
                toRunPrivate.add(inputBuffer);
                // break;
                return forwardProgress;
            }
        }

        return forwardProgress;
    }

    private void waitForMoreWork() {
        boolean local = this.somethingHappenedLocal.getAndSet(false);
        if (local) {
            return;
        } else {
            synchronized (this.somethingHappenedLocal) {
                // LOG:PROFILE
                // OpenCLDriver.logger.log("      Blocking on spillDone", this.clContext);
                while (this.somethingHappenedLocal.get() == false /* && getFirstCompleteKernel() == null */ ) {
                    try {
                        this.somethingHappenedLocal.wait();
                        // this.somethingHappenedLocal.wait();
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }
                // LOG:PROFILE
                // OpenCLDriver.logger.log("      Unblocking on spillDone", this.clContext);
                this.somethingHappenedLocal.set(false);
            }
        }
    }

    @Override
    public void run() {
        // LOG:PROFILE
        // OpenCLDriver.logger.log("Preallocating kernels", this.clContext);
        this.freeKernels.preallocateKernels();
        // LOG:PROFILE
        // OpenCLDriver.logger.log("Done reallocating kernels", this.clContext);

        // LOG:PROFILE
        // OpenCLDriver.logger.log("Waiting for first input", this.clContext);
        synchronized (this.somethingHappenedLocal) {
            while (toRun.isEmpty()) {
                try {
                    this.somethingHappenedLocal.wait();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
        }
        // LOG:PROFILE
        // OpenCLDriver.logger.log("Done waiting for first input", this.clContext);

        /*
         * I removed the condition !toWrite.isEmpty() because I'd rather
         * exit the loop and then just loop on toWrite after setting
         * usingOpencl to false (this should be more efficient so we're
         * not just constantly throwing exceptions
         */
        while (!mainDone || /* !running.isEmpty() || */
                !toRunPrivate.isEmpty() || !toCopyFromOpenCL.isEmpty() || 
                kernelsActive.get() > 0 ) {

            boolean forwardProgress = false;
            /*
             * Copy back kernels
             */
            forwardProgress |= doKernelCopyBack();

            /*
             * Output Buffer Handling
             */
            forwardProgress |= doOutputBuffers();

            /*
             * Kernel Completion Handling
             */
            // forwardProgress |= doKernelCompletion();

            /*
             * Input Buffer Handling
             */
            forwardProgress |= doInputBuffers();

            if (!forwardProgress) {
                waitForMoreWork();
            }
        }

        // LOG:DIAGNOSTIC
        // log("    At end, "+toWrite.size()+" output buffers remaining to write");
        while (!toWrite.isEmpty()) {
            boolean forwardProgress = doOutputBuffers();
            if (!forwardProgress) {
                waitForMoreWork();
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
        // sb.append("\n");
        // sb.append("  running: [ ");
        // for (Map.Entry<HadoopCLKernel, HadoopCLInputOutputBufferPair> entry : running.entrySet()) {
        //     sb.append(entry.getValue().inputBuffer().id);
        //     sb.append("->");
        //     sb.append(entry.getKey().id);
        //     sb.append(" ");
        // }
        // sb.append("]");
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
