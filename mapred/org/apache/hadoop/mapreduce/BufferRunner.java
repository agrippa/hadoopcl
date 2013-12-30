package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferRunner implements Runnable {
    private final boolean enableLogs;
    private final List<HadoopCLProfile> profiles;
    private final BufferManager<HadoopCLInputBuffer> freeInputBuffers;
    private final BufferManager<HadoopCLOutputBuffer> freeOutputBuffers; // exclusive
    private final KernelManager freeKernels; // exclusive

    private final HadoopCLLimitedQueue<HadoopCLInputBuffer> toRun;
    private final LinkedList<HadoopCLInputBuffer> toRunPrivate; // exclusive
    private final LinkedList<OutputBufferSoFar> toWrite; // exclusive
    private final LinkedList<HadoopCLKernel> toCopyFromOpenCL; // exclusive

    public static final AtomicBoolean somethingHappened = new AtomicBoolean(false);
    private boolean mainDone;

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
        this.toCopyFromOpenCL = new LinkedList<HadoopCLKernel>();

        this.running = new HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair>();

        this.clContext = clContext;
        this.profiles = new LinkedList<HadoopCLProfile>();

        this.enableLogs = clContext.enableBufferRunnerDiagnostics();
        this.mainDone = false;
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

        synchronized (BufferRunner.somethingHappened) {
            this.toRun.add(input);
            BufferRunner.somethingHappened.set(true);
            BufferRunner.somethingHappened.notify();
        }
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
            HadoopCLInputBuffer inputBuffer) {
        boolean success;

        kernel.tracker = inputBuffer.tracker.clone();
        // outputBuffer.tracker = inputBuffer.tracker.clone();
        kernel.fill(inputBuffer);
        try {
            OpenCLDriver.logger.log("launching kernel "+
                kernel.tracker.toString()+" on "+inputBuffer.tracker.toString(),
                this.clContext);
            success = kernel.launchKernel();
        } catch(Exception io) {
            throw new RuntimeException(io);
        }
        if (success) {
            kernel.openclProfile = inputBuffer.getProfile();
            kernel.openclProfile.startKernel();
            running.put(kernel, new HadoopCLInputOutputBufferPair(kernel));
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

    private void handleOpenCLCopy(HadoopCLKernel complete,
            HadoopCLOutputBuffer output) {
        complete.prepareForRead(output);
        complete.waitFor();

        output.copyOverFromKernel(complete);
        output.tracker = complete.tracker.clone();

        log("    Adding "+output.id+" to output buffers to write");

        boolean completedAll = output.completedAll();
        toWrite.add(new OutputBufferSoFar(output, 0));

        if (!completedAll) {
        // if (output.memRetry[0] != 0) {
            log("      Retrying kernel "+complete.id+" due to memRetry="+output.memRetry[0]);
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
            complete.openclProfile.startKernel();
            running.put(complete, new HadoopCLInputOutputBufferPair(complete));
        } else {
            log("      Releasing kernel "+complete.id+" due to memRetry="+output.memRetry[0]);
            freeKernels.free(complete);
        }
    }

    private boolean doOutputBuffers() {
        boolean forwardProgress = false;

        OutputBufferSoFar soFar = null;
        while ((soFar = toWrite.poll()) != null) {
            log("    Got output buffer "+soFar.buffer().id+" to write");

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

    private boolean doKernelCompletion() {
        boolean forwardProgress = false;

        List<HadoopCLKernel> completed = getCompleteKernels();

        for (HadoopCLKernel complete : completed) {
            // Try to either re-run incomplete kernels, or just
            // set the output buffers up for dumping
            final HadoopCLInputOutputBufferPair pair = running.remove(complete);
            forwardProgress = true;

            log("  Detected completed kernel "+complete.id);
            OpenCLDriver.logger.log("recovering completed kernel "+
                complete.tracker.toString(), this.clContext);

            complete.openclProfile.stopKernel();
            try {
              pair.wrapperThread().join();
            } catch(InterruptedException ie) {
              throw new RuntimeException(ie);
            }

            HadoopCLOutputBuffer output =
                allocOutputBufferWithInit(complete.getOutputPairsPerInput());

            if (output == null) {
                toCopyFromOpenCL.add(complete);
            } else {
                handleOpenCLCopy(complete, output);
            }
        }

        return forwardProgress;
    }

    private HadoopCLInputBuffer getInputBuffer() {

        HadoopCLInputBuffer inputBuffer = null;
        BufferTypeContainer<HadoopCLInputBuffer> inputBufferContainer = 
            toRun.nonBlockingGet();

        if (inputBufferContainer != null) {
            if (inputBufferContainer.get() == null) {
                log("   Got DONE signal from main");
                this.mainDone = true;
            } else {
                inputBuffer = inputBufferContainer.get();
                log("  Got input buffer "+inputBuffer.id+" from main");
            }
        }

        if (inputBuffer == null) {
            // try again for any retry input buffers
            if (!toRunPrivate.isEmpty()) {
                inputBuffer = toRunPrivate.poll();
            }
            if (inputBuffer != null) {
                log("  Got input buffer "+inputBuffer.id+" from retry list");
            }
        }

        return inputBuffer;
    }

    private boolean doInputBuffers() {
        boolean forwardProgress = false;

        HadoopCLInputBuffer inputBuffer;
        while ((inputBuffer = getInputBuffer()) != null) {
            HadoopCLKernel k = newKernelInstance();
            if (k != null) {
                log("    Allocated kernel "+k.id+" for processing of input buffer "+inputBuffer.id);
                
                if (!startKernel(k, inputBuffer)) {
                    log("    Failed to start kernel, marking input "+inputBuffer.id+" to retry and freeing kernel "+k.id);
                    toRunPrivate.add(inputBuffer);
                    freeKernels.free(k);
                } else {
                    forwardProgress = true;
                    profiles.add(inputBuffer.getProfile());
                    freeInputBuffers.free(inputBuffer);
                    log("    Successfully started kernel "+k.id+" on "+inputBuffer.id);
                }
            } else {
                log("    Failed to allocate kernel, marking "+inputBuffer.id+" for retry");
                toRunPrivate.add(inputBuffer);
                break;
            }
        }

        return forwardProgress;
    }

    private void waitForMoreWork() {
        boolean local = BufferRunner.somethingHappened.getAndSet(false);
        if (local) {
            return;
        } else {
            synchronized (BufferRunner.somethingHappened) {
                while (BufferRunner.somethingHappened.get() == false) {
                    try {
                        BufferRunner.somethingHappened.wait();
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }
                BufferRunner.somethingHappened.set(false);
            }
        }
    }

    @Override
    public void run() {

        synchronized (BufferRunner.somethingHappened) {
            while (toRun.isEmpty()) {
                try {
                    BufferRunner.somethingHappened.wait();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
        }

        /*
         * I removed the condition !toWrite.isEmpty() because I'd rather
         * exit the loop and then just loop on toWrite after setting
         * usingOpencl to false (this should be more efficient so we're
         * not just constantly throwing exceptions
         */
        while (!mainDone || !running.isEmpty() ||
                !toRunPrivate.isEmpty() || !toCopyFromOpenCL.isEmpty()) {

            boolean forwardProgress = false;
            /*
             * Output Buffer Handling
             */
            forwardProgress |= doOutputBuffers();

            /*
             * Copy back kernels
             */
            forwardProgress |= doKernelCopyBack();

            /*
             * Kernel Completion Handling
             */
            forwardProgress |= doKernelCompletion();

            /*
             * Input Buffer Handling
             */
            forwardProgress |= doInputBuffers();

            if (!forwardProgress) {
                waitForMoreWork();
            }
        }

        log("    At end, "+toWrite.size()+" output buffers remaining to write");
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
