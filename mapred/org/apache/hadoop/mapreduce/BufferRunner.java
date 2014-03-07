package org.apache.hadoop.mapreduce;

import com.amd.aparapi.internal.kernel.KernelRunner;
import com.amd.aparapi.Kernel;
import com.amd.aparapi.device.OpenCLDevice;

import static org.apache.hadoop.mapred.Task.Counter.COMBINE_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.SPILLED_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.COMBINE_OUTPUT_RECORDS;

import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.SortedWriter;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.mapred.SpillRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskTracker;

import java.util.ArrayList;
import java.util.Deque;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
 * Main state storage:
 *   toCopyFromOpenCL
 *   toWrite
 *   toRun
 */
public class BufferRunner implements Runnable {
    private final boolean enableLogs;
    private final ConcurrentLinkedQueue<HadoopCLProfile> profiles;
    private final ConcurrentLinkedQueue<HadoopCLInputBuffer> freeInputBuffers;
    private final BufferManager<HadoopCLOutputBuffer> freeOutputBuffers; // exclusive
    private volatile int kernelThreadsActive;

    private final ConcurrentLinkedQueue<HadoopCLInputBuffer> toRun;
    private final Deque<OutputBufferSoFar> toWrite;
    private final ConcurrentLinkedQueue<HadoopCLKernel> toCopyFromOpenCL;

    public static final AtomicBoolean somethingHappened = new AtomicBoolean(false);
    private final AtomicBoolean somethingHappenedLocal;
    private boolean mainDone;

    private final HadoopOpenCLContext clContext;

    public BufferRunner(ConcurrentLinkedQueue<HadoopCLInputBuffer> freeInputBuffers,
            BufferManager<HadoopCLOutputBuffer> freeOutputBuffers,
            HadoopOpenCLContext clContext) {
        this.freeInputBuffers = freeInputBuffers;
        this.freeOutputBuffers = freeOutputBuffers;

        this.toRun = new ConcurrentLinkedQueue<HadoopCLInputBuffer>();
        this.toWrite = new LinkedList<OutputBufferSoFar>();
        this.toCopyFromOpenCL = new ConcurrentLinkedQueue<HadoopCLKernel>();

        this.clContext = clContext;
        this.profiles = new ConcurrentLinkedQueue<HadoopCLProfile>();

        this.enableLogs = clContext.enableBufferRunnerDiagnostics();
        this.mainDone = false;

        if (this.clContext.isCombiner()) {
            this.somethingHappenedLocal = new AtomicBoolean(false);
        } else {
            this.somethingHappenedLocal = somethingHappened;
        }
    }

    public ConcurrentLinkedQueue<HadoopCLProfile> profiles() {
        return this.profiles;
    }

    private void log(String s) {
        if (enableLogs) {
            System.err.println(System.currentTimeMillis()+"|"+this.clContext.verboseTypeName()+" "+s);
        }
    }

    public void addWork(HadoopCLInputBuffer input) {
        // LOG:DIAGNOSTIC
        // log("Placing input buffer "+(input == null ? "null" : input.id)+" from main");
        synchronized (toRun) {
            this.toRun.add(input);
            this.toRun.notify();
        }
    }
    
    private boolean startKernel(final HadoopCLKernel kernel,
            HadoopCLInputBuffer inputBuffer) {
        boolean success;

        kernel.tracker = inputBuffer.tracker.clone();
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
            kernel.openclProfile = inputBuffer.getProfile();
            kernel.openclProfile.startKernel();
        }
        return success;
    }

    private OutputBufferSoFar handleOutputBuffer(OutputBufferSoFar soFar) {
        try {
            final HadoopCLProfile prof = soFar.buffer().getProfile();
            prof.startWrite(soFar.buffer());
            final int newProgress = soFar.buffer().putOutputsIntoHadoop(
                    this.clContext.getContext(), soFar.soFar());
            prof.stopWrite(soFar.buffer());
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

    private HadoopCLOutputBuffer allocOutputBufferWithInit() {
        HadoopCLOutputBuffer result = null;
        final BufferManager.TypeAlloc<HadoopCLOutputBuffer> outputBufferContainer =
            this.freeOutputBuffers.alloc();
        if (outputBufferContainer != null) {
          result = outputBufferContainer.obj();
        }
        return result;
    }

    private void handleOpenCLCopy(HadoopCLKernel complete,
            HadoopCLOutputBuffer output) {
        // LOG:PROFILE
        // OpenCLDriver.logger.log("started reading from opencl", this.clContext);
        // LOG:DIAGNOSTIC
        // log("    Reading kernel "+complete.id+" into output buffer "+output.id);
        complete.prepareForRead(output);
        complete.waitFor();

        output.copyOverFromKernel(complete);
        output.tracker = complete.tracker.clone();
        // LOG:PROFILE
        // OpenCLDriver.logger.log("done reading from opencl", this.clContext);

        // LOG:DIAGNOSTIC
        // log("    Adding "+output.id+" to output buffers to write");
        boolean completedAll = output.completedAll();

        synchronized (complete) {
            complete.setAvailable(true);
            complete.notify();
        }

        // if (this.clContext.isMapper()) {
        //     output.printContents();
        // }
        toWrite.addLast(new OutputBufferSoFar(output, 0));
    }

    private boolean doSingleOutputBuffer(OutputBufferSoFar soFar) {
        boolean forwardProgress = false;
        // LOG:DIAGNOSTIC
        // log("    Got output buffer "+soFar.buffer().id+" to write");

        int count = soFar.buffer().getCount();
        int previously = soFar.soFar();
        OutputBufferSoFar cont = handleOutputBuffer(soFar);
        if (cont != null) {
            if (cont.soFar() > previously) {
                forwardProgress = true;
            }
            this.toWrite.addFirst(cont);
        } else {
            forwardProgress = true;
        }

        return forwardProgress;
    }

    private boolean doAllOutputBuffers() {
        boolean forwardProgress = false;

        while (!toWrite.isEmpty()) {
            final int sizeBefore = this.toWrite.size();
            final OutputBufferSoFar soFar = toWrite.removeFirst();
            // LOG:DIAGNOSTIC
            // log("    Got output buffer "+soFar.buffer().id+" to write");
            forwardProgress |= doSingleOutputBuffer(soFar);
            if (sizeBefore == this.toWrite.size()) break;
        }
        return forwardProgress;
    }

    private boolean doOutputBuffers() {
        boolean forwardProgress = false;

        if (!toWrite.isEmpty()) {
            final OutputBufferSoFar soFar = toWrite.removeFirst();
            // LOG:DIAGNOSTIC
            // log("    Got output buffer "+soFar.buffer().id+" to write");
            forwardProgress |= doSingleOutputBuffer(soFar);
        }

        return forwardProgress;
    }

    private boolean doKernelCopyBack() {
        boolean forwardProgress = false;
        do {
            final HadoopCLKernel kernel = toCopyFromOpenCL.poll();
            if (kernel == null) break;

            HadoopCLOutputBuffer output =
                allocOutputBufferWithInit();
            if (output == null) {
                toCopyFromOpenCL.add(kernel);
                break;
            }

            forwardProgress = true;
            handleOpenCLCopy(kernel, output);
        } while (true);

        return forwardProgress;
    }

    private List<IterAndBuffers> waitForMoreWork() {
        List<IterAndBuffers> complete = null;
        boolean local = this.somethingHappenedLocal.getAndSet(false);
        if (local) {
            return null;
        } else {
            // LOG:DIAGNOSTIC
            // log("Waiting for more work");
            synchronized (this.somethingHappenedLocal) {
                // LOG:PROFILE
                // OpenCLDriver.logger.log("      Blocking on spillDone", this.clContext);
                while (this.somethingHappenedLocal.get() == false) {
                    try {
                        this.somethingHappenedLocal.wait();
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }
                // LOG:PROFILE
                // OpenCLDriver.logger.log("      Unblocking on spillDone", this.clContext);

                this.somethingHappenedLocal.set(false);
            }
            // LOG:DIAGNOSTIC
            // log("Done waiting for more work");
        }
        return complete;
    }

    @Override
    public void run() {
        // LOG:PROFILE
        // OpenCLDriver.logger.log("Preallocating kernels", this.clContext);
        Thread[] kernelThreads = new Thread[this.clContext.getNKernels()];
        this.kernelThreadsActive = this.clContext.getNKernels();

        for (int i = 0; i < this.clContext.getNKernels(); i++) {
            final HadoopCLKernel kernel;
            try {
                kernel = this.clContext.thisKernelConstructor.newInstance(
                        this.clContext, i);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            kernel.doEntrypointInit(this.clContext.getDevice(),
                this.clContext.getDeviceSlot(),
                this.clContext.getContext().getTaskAttemptID().getTaskID().getId(),
                this.clContext.getContext().getTaskAttemptID().getId());

            kernelThreads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        final HadoopCLInputBuffer input;
                        synchronized (toRun) {
                            try {
                                while (toRun.isEmpty()) {
                                    toRun.wait();
                                }
                            } catch (InterruptedException ie) {
                                throw new RuntimeException(ie);
                            }
                            if (toRun.peek() instanceof MainDoneMarker) {
                                // LOG:DIAGNOSTIC
                                // log("   Got DONE signal from main");
                                toRun.notify();
                                break;
                            } else {
                                input = toRun.poll();
                                // LOG:DIAGNOSTIC
                                // log("  Got input buffer "+input.id+" from main");
                            }
                        }
                        input.clearNWrites();

                        if (!startKernel(kernel, input)) {
                            throw new RuntimeException("Failed to start kernel");
                        }
                        profiles.add(input.getProfile());
                        synchronized (freeInputBuffers) {
                            freeInputBuffers.add(input);
                            freeInputBuffers.notify();
                        }

                        int willRequireRestart = kernel.waitForCompletion();
                        // LOG:DIAGNOSTIC
                        // log("  Detected completed kernel "+kernel.id);

                        // LOG:PROFILE
                        // OpenCLDriver.logger.log("recovering completed kernel "+kernel.tracker.toString(), clContext);

                        kernel.openclProfile.stopKernel();
                        synchronized (kernel) {
                            synchronized (somethingHappenedLocal) {
                                toCopyFromOpenCL.add(kernel);
                                somethingHappenedLocal.set(true);
                                somethingHappenedLocal.notify();
                            }

                            kernel.setAvailable(false);
                            try {
                                do {
                                    kernel.wait();
                                } while (!kernel.getAvailable());
                            } catch (InterruptedException ie) {
                                throw new RuntimeException(ie);
                            }
                        }

                        while (willRequireRestart != 0) {
                            // LOG:DIAGNOSTIC
                            // log("      Retrying kernel "+kernel.id+" due to willRequireRestart="+willRequireRestart);
                            kernel.tracker.incrementAttempt();
                            // LOG:PROFILE
                            // OpenCLDriver.logger.log("relaunching kernel "+kernel.tracker.toString(), clContext);
                            try {
                                if (!kernel.relaunchKernel()) {
                                    throw new RuntimeException("Failure to re-launch kernel");
                                }
                            } catch (IOException io) {
                                throw new RuntimeException(io);
                            } catch (InterruptedException ie) {
                                throw new RuntimeException(ie);
                            }
                            kernel.openclProfile.startKernel();
                            willRequireRestart = kernel.waitForCompletion();
                            kernel.openclProfile.stopKernel();
                            // LOG:PROFILE
                            // OpenCLDriver.logger.log("recovering relaunched kernel "+kernel.tracker.toString(), clContext);    

                            // LOG:DIAGNOSTIC
                            // log("  Detected completed kernel "+kernel.id);

                            synchronized (kernel) {
                                synchronized (somethingHappenedLocal) {
                                    toCopyFromOpenCL.add(kernel);
                                    somethingHappenedLocal.set(true);
                                    somethingHappenedLocal.notify();
                                }

                                kernel.setAvailable(false);
                                try {
                                    do {
                                        kernel.wait();
                                    } while (!kernel.getAvailable());
                                } catch (InterruptedException ie) {
                                    throw new RuntimeException(ie);
                                }
                            }
                        }
                    }

                    synchronized (somethingHappenedLocal) {
                        kernelThreadsActive--;
                        somethingHappenedLocal.set(true);
                        somethingHappenedLocal.notify();
                    }
                    kernel.dispose();
                }
            });
            kernelThreads[i].setName("KernelThread-"+i);
            kernelThreads[i].start();
        }

        OpenCLDevice combinerDevice;
        if (this.clContext.isMapper() && this.clContext.jobHasCombiner() &&
                (combinerDevice = this.clContext.getCombinerDevice()) != null) {
            KernelRunner.doKernelAndArgLinesPrealloc(
                this.clContext.getCombinerKernel(),
                Kernel.TaskType.COMBINER, this.clContext.nCombinerKernels(),
                combinerDevice, this.clContext.getDeviceSlot());
        }
        // LOG:PROFILE
        // OpenCLDriver.logger.log("Done reallocating kernels", this.clContext);

        /*
         * I removed the condition !toWrite.isEmpty() because I'd rather
         * exit the loop and then just loop on toWrite after setting
         * usingOpencl to false (this should be more efficient so we're
         * not just constantly throwing exceptions
         */
        boolean forwardProgress = false;

        while (this.kernelThreadsActive > 0 || !toCopyFromOpenCL.isEmpty() ||
                !toWrite.isEmpty()) {
            if (!forwardProgress) {
                waitForMoreWork();
            }
            forwardProgress = false;

            forwardProgress |= doKernelCopyBack();
            forwardProgress |= doOutputBuffers();
        }

        try {
            for (int i = 0; i < this.clContext.getNKernels(); i++) {
                kernelThreads[i].join();
            }
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
        // LOG:DIAGNOSTIC
        // log("BufferRunner exiting");
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
        sb.append("]\n");
        return sb.toString();
    }

    public static class OutputBufferSoFar {
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

    private static class IterAndBuffers {
        public final HadoopCLKeyValueIterator iter;
        public final List<OutputBufferSoFar> buffers;

        public IterAndBuffers(HadoopCLKeyValueIterator iter,
                List<OutputBufferSoFar> buffers) {
            this.iter = iter;
            this.buffers = buffers;
        }
    }
}
