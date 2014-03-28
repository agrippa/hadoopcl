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

import java.util.Queue;
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
    private final List<HadoopCLProfile> profiles;
    private final List<HadoopCLInputBuffer> freeInputBuffers;
    private final List<HadoopCLOutputBuffer> freeOutputBuffers;
    private final Queue<HadoopCLInputBuffer> toRun;
    private final Deque<HadoopCLOutputBuffer> toWrite;
    private final Queue<HadoopCLKernel> toCopyFromOpenCL;

    public static final AtomicBoolean somethingHappened = new AtomicBoolean(false);
    private final AtomicBoolean somethingHappenedLocal;

    private final HadoopOpenCLContext clContext;

    public BufferRunner(List<HadoopCLInputBuffer> freeInputBuffers,
            HadoopOpenCLContext clContext) {
        this.freeInputBuffers = freeInputBuffers;
        this.freeOutputBuffers = new LinkedList<HadoopCLOutputBuffer>();

        this.toRun = new LinkedList<HadoopCLInputBuffer>();
        this.toWrite = new LinkedList<HadoopCLOutputBuffer>();
        this.toCopyFromOpenCL = new LinkedList<HadoopCLKernel>();

        this.clContext = clContext;
        this.profiles = new LinkedList<HadoopCLProfile>();

        this.enableLogs = clContext.enableBufferRunnerDiagnostics();

        if (this.clContext.isCombiner()) {
            this.somethingHappenedLocal = new AtomicBoolean(false);
        } else {
            this.somethingHappenedLocal = somethingHappened;
        }
    }

    public List<HadoopCLProfile> profiles() {
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

    private int handleOutputBuffer(HadoopCLOutputBuffer buffer, int soFar) {
        try {
            final HadoopCLProfile prof = buffer.getProfile();
            prof.startWrite(buffer);
            final int newProgress = buffer.putOutputsIntoHadoop(
                    this.clContext.getContext(), soFar);
            prof.stopWrite(buffer);
            if (newProgress == -1) {
                // LOG:DIAGNOSTIC
                // log("    Done writing "+buffer.id+", releasing");
                synchronized (freeOutputBuffers) {
                    this.freeOutputBuffers.add(buffer);
                    freeOutputBuffers.notify();
                }
            }
            return newProgress;
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
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

        synchronized (complete) {
            complete.setAvailable(true);
            complete.notify();
        }

        // LOG:PROFILE
        // OpenCLDriver.logger.log("done reading from opencl", this.clContext);
        // LOG:DIAGNOSTIC
        // log("    Adding "+output.id+" to output buffers to write");

        // if ((clContext.isCombiner() || clContext.isReducer()) && ((ReduceContext)clContext.getContext()).shouldPrint()) {
        // if (this.clContext.isMapper()) {
        //     output.printContents();
        // }
        synchronized (toWrite) {
            toWrite.addLast(output);
            toWrite.notify();
        }
    }

    private void waitForMoreWork() {
        boolean local = this.somethingHappenedLocal.getAndSet(false);
        if (local) {
            return;
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
    }

    private final Thread[] constructKernelThreads(final Thread[] kernelThreads,
            final HadoopCLKernel kernels[]) {

        try {
            for (int i = 0; i < this.clContext.getNKernels(); i++) {
                final HadoopCLKernel kernel =
                    this.clContext.thisKernelConstructor.newInstance(
                            this.clContext, i);;

                kernels[i] = kernel;
                kernelThreads[i] = new Thread(new Runnable() {

                    private final void copyBackKernel() {
                        synchronized (kernel) {
                            synchronized (toCopyFromOpenCL) {
                                toCopyFromOpenCL.add(kernel);
                                toCopyFromOpenCL.notify();
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

                    @Override
                    public void run() {
                        kernel.doEntrypointInit(clContext.getDevice(),
                            clContext.getDeviceSlot(),
                            clContext.getContext().getTaskAttemptID().getTaskID().getId(),
                            clContext.getContext().getTaskAttemptID().getId());

                        final List<HadoopCLProfile> localProfiles = new LinkedList<HadoopCLProfile>();
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
                            // if ((clContext.isCombiner() || clContext.isReducer()) && !((ReduceContext)clContext.getContext()).shouldPrint()) {
                            //     input.printContents();
                            // }
                            input.clearNWrites();

                            final long nInputs = input.getNInputs();
                            final long startProcessing = System.currentTimeMillis();
                            if (!startKernel(kernel, input)) {
                                throw new RuntimeException("Failed to start kernel");
                            }
                            localProfiles.add(input.getProfile());
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

                            copyBackKernel();
                            final long stopProcessing = System.currentTimeMillis();
                            if (!clContext.isCombiner()) {
                                OpenCLDriver.addProgress(nInputs,
                                    stopProcessing - startProcessing);
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

                                copyBackKernel();
                            }
                        }

                        synchronized (profiles) {
                            profiles.addAll(localProfiles);
                        }

                        synchronized (somethingHappenedLocal) {
                            somethingHappenedLocal.set(true);
                            somethingHappenedLocal.notify();
                        }
                        kernel.dispose();

                        synchronized (toCopyFromOpenCL) {
                            toCopyFromOpenCL.add(new KernelThreadDone(clContext, -1));
                            toCopyFromOpenCL.notify();
                        }
                    }
                });
                kernelThreads[i].setName("KernelThread-"+i);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return kernelThreads;
    }

    private final Thread constructCopyFromOpenCLThread() {
        final Thread copyThread = new Thread() {
            @Override
            public void run() {
                final int totalNKernels = clContext.getNKernels();
                int kernelDoneSignals = 0;
                while (kernelDoneSignals < totalNKernels) {
                    try {
                        final HadoopCLKernel kernel;
                        synchronized (toCopyFromOpenCL) {
                            while (toCopyFromOpenCL.isEmpty()) {
                                toCopyFromOpenCL.wait();
                            }
                            kernel = toCopyFromOpenCL.poll();
                        }
                        if (kernel instanceof KernelThreadDone) {
                            kernelDoneSignals++;
                        } else {
                            HadoopCLOutputBuffer output = null;
                            synchronized (freeOutputBuffers) {
                                while (freeOutputBuffers.isEmpty()) {
                                    freeOutputBuffers.wait();
                                }
                                output = freeOutputBuffers.remove(0);
                            }
                            handleOpenCLCopy(kernel, output);
                        }
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }

                synchronized (toWrite) {
                    toWrite.addLast(new CopyThreadDone(clContext, -1));
                    toWrite.notify();
                }
            }
        };
        copyThread.setName("CopyThread");
        return copyThread;
    }

    @Override
    public void run() {
        // LOG:PROFILE
        // OpenCLDriver.logger.log("Preallocating kernels", this.clContext);

        try {
            for (int i = 0; i < this.clContext.getNOutputBuffers(); i++) {
                freeOutputBuffers.add(
                    this.clContext.outputBufferConstructor.newInstance(
                        this.clContext, i));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        final Thread[] kernelThreads = new Thread[this.clContext.getNKernels()];
        final HadoopCLKernel[] kernels = new HadoopCLKernel[this.clContext.getNKernels()];
        constructKernelThreads(kernelThreads, kernels);

        for (Thread t : kernelThreads) {
            t.start();
        }

        final Thread copyThread = constructCopyFromOpenCLThread();
        copyThread.start();

        OpenCLDevice combinerDevice;
        if (this.clContext.isMapper() && this.clContext.jobHasCombiner() &&
                (combinerDevice = this.clContext.getCombinerDevice()) != null) {
            KernelRunner.doKernelAndArgLinesPrealloc(
                this.clContext.getCombinerKernel(),
                Kernel.TaskType.COMBINER, this.clContext.nCombinerKernels(),
                combinerDevice, this.clContext.getDeviceSlot());
        }

        // LOG:PROFILE
        // OpenCLDriver.logger.log("Done preallocating kernels", this.clContext);

        /*
         * I removed the condition !toWrite.isEmpty() because I'd rather
         * exit the loop and then just loop on toWrite after setting
         * usingOpencl to false (this should be more efficient so we're
         * not just constantly throwing exceptions
         */
        boolean copyDone = false;

        while (!copyDone || !toWrite.isEmpty()) {

            final HadoopCLOutputBuffer outputBuffer;
            synchronized (toWrite) {
                try {
                    while (toWrite.isEmpty()) {
                        toWrite.wait();
                    }
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
                outputBuffer = toWrite.removeFirst();
            }

            if (outputBuffer instanceof CopyThreadDone) {
                copyDone = true;
                continue;
            } else {
                int progress = handleOutputBuffer(outputBuffer, 0);
                while (progress != -1) {
                    waitForMoreWork();
                    progress = handleOutputBuffer(outputBuffer, progress);
                }
            }
        }

        try {
            for (int i = 0; i < this.clContext.getNKernels(); i++) {
                kernelThreads[i].join();
            }
            copyThread.join();
            for (int i = 0; i < this.clContext.getNKernels(); i++) {
                kernels[i].dispose();
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
        // sb.append("  freeOutputs: ");
        // sb.append(this.freeOutputBuffers.toString());
        // sb.append("\n");
        sb.append("  toRun: ");
        sb.append(this.toRun.toString());
        sb.append("\n");
        sb.append("]\n");
        return sb.toString();
    }
}
