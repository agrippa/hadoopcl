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
    private final ConcurrentLinkedQueue<HadoopCLInputBuffer> freeInputBuffers;
    private final BufferManager<HadoopCLOutputBuffer> freeOutputBuffers; // exclusive
    // private final KernelManager freeKernels; // exclusive
    // private final LinkedList<HadoopCLKernel> freeKernels;
    private final ConcurrentLinkedQueue<HadoopCLKernel> freeKernels;

    private final ConcurrentLinkedQueue<HadoopCLInputBuffer> toRun;
    private final LinkedList<HadoopCLInputBuffer> toRunPrivate; // exclusive
    private final Deque<OutputBufferSoFar> toWrite;
    // private final LinkedList<OutputBufferSoFar> toWrite; // exclusive
    private final LinkedList<HadoopCLKernel> toCopyFromOpenCL;

    public static final AtomicBoolean somethingHappened = new AtomicBoolean(false);
    // public static final AtomicBoolean somethingHappenedCombiner = new AtomicBoolean(false);
    private final AtomicBoolean somethingHappenedLocal;
    private boolean mainDone;

    // exclusive
    // private final List<HadoopCLKernel> running;
    private final AtomicInteger kernelsActive;

    private final HadoopOpenCLContext clContext;

    public BufferRunner(ConcurrentLinkedQueue<HadoopCLInputBuffer> freeInputBuffers,
            BufferManager<HadoopCLOutputBuffer> freeOutputBuffers,
            // LinkedList<HadoopCLKernel> freeKernels,
            ConcurrentLinkedQueue<HadoopCLKernel> freeKernels,
            HadoopOpenCLContext clContext) {
        this.freeInputBuffers = freeInputBuffers;
        this.freeOutputBuffers = freeOutputBuffers;
        this.freeKernels = freeKernels;

        this.toRun = new ConcurrentLinkedQueue<HadoopCLInputBuffer>();
        this.toRunPrivate = new LinkedList<HadoopCLInputBuffer>();
        this.toWrite = new LinkedList<OutputBufferSoFar>();
        this.toCopyFromOpenCL = new LinkedList<HadoopCLKernel>();

        kernelsActive = new AtomicInteger();

        this.clContext = clContext;
        this.profiles = new LinkedList<HadoopCLProfile>();

        this.enableLogs = clContext.enableBufferRunnerDiagnostics();
        this.mainDone = false;

        if (this.clContext.isCombiner()) {
            // this.somethingHappenedLocal = somethingHappenedCombiner;
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

        // possible if getting DONE signal from main
        HadoopCLKernel k = null;
        if (!(input instanceof MainDoneMarker)) {
            input.clearNWrites();

            k = newKernelInstance();
            if (k != null) {
                // LOG:DIAGNOSTIC
                // log("    Allocated kernel "+k.id+" for processing of input buffer "+input.id);
                if (startKernel(k, input)) {
                    // LOG:DIAGNOSTIC
                    // log("    Successfully started kernel "+k.id+" on "+input.id);
                    profiles.add(input.getProfile());
                    freeInputBuffers.add(input);
                    return;
                } else {
                    // LOG:DIAGNOSTIC
                    // log("    Failed to start kernel, marking input "+input.id+" to retry and freeing kernel "+k.id);
                }
            }
        }

        synchronized (this.somethingHappenedLocal) {
            if (k != null) {
                freeKernels.add(k);
            }
            this.toRun.add(input);
            this.somethingHappenedLocal.set(true);
            this.somethingHappenedLocal.notify();
        }
    }

    private HadoopCLKernel newKernelInstance() {
        return freeKernels.poll();
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

    private void spawnKernelTrackingThread(final HadoopCLKernel kernel, final boolean relaunch) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                kernel.waitForCompletion();
                // LOG:DIAGNOSTIC
                // log("  Detected completed kernel "+kernel.id);
                if (relaunch) {
                    // LOG:PROFILE
                    // OpenCLDriver.logger.log("recovering relaunched kernel "+kernel.tracker.toString(), clContext);
                } else {
                    // LOG:PROFILE
                    // OpenCLDriver.logger.log("recovering completed kernel "+kernel.tracker.toString(), clContext);
                }
                kernel.openclProfile.stopKernel();
                synchronized (somethingHappenedLocal) {
                    toCopyFromOpenCL.add(kernel);
                    kernelsActive.getAndDecrement();
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
            spawnKernelTrackingThread(kernel, false);
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

    private HadoopCLOutputBuffer allocOutputBufferWithInit() {
        HadoopCLOutputBuffer result = null;
        BufferManager.TypeAlloc<HadoopCLOutputBuffer> outputBufferContainer =
            freeOutputBuffers.alloc();
        if (outputBufferContainer != null) {
          result = outputBufferContainer.obj();
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
        toWrite.addLast(new OutputBufferSoFar(output, 0));

        if (!completedAll) {
            // LOG:DIAGNOSTIC
            // log("      Retrying kernel "+complete.id+" due to completedAll="+completedAll);
            complete.tracker.incrementAttempt();
            // LOG:PROFILE
            // OpenCLDriver.logger.log("relaunching kernel "+complete.tracker.toString(), this.clContext);
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
            kernelsActive.getAndIncrement();
            spawnKernelTrackingThread(complete, true);
        } else {
            // LOG:DIAGNOSTIC
            // log("      Releasing kernel "+complete.id+" due to completedAll="+completedAll);
            freeKernels.add(complete);
        }
    }

    private boolean doSingleOutputBuffer(OutputBufferSoFar soFar) {
        boolean forwardProgress = false;
        // LOG:DIAGNOSTIC
        // log("    Got output buffer "+soFar.buffer().id+" to write");

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

    private boolean doOutputBuffers() {
        boolean forwardProgress = false;

        if (!toWrite.isEmpty()) {
        // while (!toWrite.isEmpty()) {
            int sizeBefore = this.toWrite.size();
            final OutputBufferSoFar soFar = toWrite.removeFirst();
            // LOG:DIAGNOSTIC
            // log("    Got output buffer "+soFar.buffer().id+" to write");
            forwardProgress |= doSingleOutputBuffer(soFar);
            // if (sizeBefore == this.toWrite.size()) break;
        }

        return forwardProgress;
    }

    private boolean doKernelCopyBack() {
        boolean forwardProgress = false;
        do {
            HadoopCLKernel kernel = toCopyFromOpenCL.poll();
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

    private HadoopCLInputBuffer getInputBuffer() {

        HadoopCLInputBuffer inputBuffer = toRun.poll();
        // BufferTypeContainer<HadoopCLInputBuffer> inputBufferContainer = 
        //     toRun.nonBlockingGet();

        if (inputBuffer != null) {
            if (inputBuffer instanceof MainDoneMarker) {
                // LOG:DIAGNOSTIC
                // log("   Got DONE signal from main");
                this.mainDone = true;
                inputBuffer = null;
            } else {
                // LOG:DIAGNOSTIC
                // log("  Got input buffer "+inputBuffer.id+" from main");
            }
        }
        // if (inputBufferContainer != null) {
        //     if (inputBufferContainer.get() == MainDoneMarker.SINGLETON) {
        //         // LOG:DIAGNOSTIC
        //         // log("   Got DONE signal from main");
        //         this.mainDone = true;
        //     } else {
        //         inputBuffer = inputBufferContainer.get();
        //         // LOG:DIAGNOSTIC
        //         // log("  Got input buffer "+inputBuffer.id+" from main");
        //     }
        // }

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
        while ((inputBuffer = getInputBuffer()) != null) {
            HadoopCLKernel k = newKernelInstance();
            if (k != null) {
                // LOG:DIAGNOSTIC
                // log("    Allocated kernel "+k.id+" for processing of input buffer "+inputBuffer.id);
                
                if (!startKernel(k, inputBuffer)) {
                    // LOG:DIAGNOSTIC
                    // log("    Failed to start kernel, marking input "+inputBuffer.id+" to retry and freeing kernel "+k.id);
                    toRunPrivate.add(inputBuffer);
                    freeKernels.add(k);
                    break;
                } else {
                    // LOG:DIAGNOSTIC
                    // log("    Successfully started kernel "+k.id+" on "+inputBuffer.id);
                    forwardProgress = true;
                    profiles.add(inputBuffer.getProfile());
                    synchronized(freeInputBuffers) {
                        freeInputBuffers.add(inputBuffer);
                        freeInputBuffers.notify();
                    }
                }
            } else {
                // LOG:DIAGNOSTIC
                // log("    Failed to allocate kernel, marking "+inputBuffer.id+" for retry");
                toRunPrivate.add(inputBuffer);
                break;
            }
        }
        return forwardProgress;
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
                while (this.somethingHappenedLocal.get() == false /* && getFirstCompleteKernel() == null */ ) {
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

    private void spillAll() {
        if (this.clContext.isMapper()) {
            FSDataOutputStream out = null;
            try {
                final Counter combineInputCounter = 
                  this.clContext.getContext().getCounter(COMBINE_INPUT_RECORDS);
                final Counter spilledRecordsCounter =
                  this.clContext.getContext().getCounter(SPILLED_RECORDS);
                final Counter combineOutputCounter =
                  this.clContext.getContext().getCounter(COMBINE_OUTPUT_RECORDS);

                final int partitions = this.clContext.getContext().getNumReduceTasks();
                final FileSystem rfs = ((LocalFileSystem)FileSystem.getLocal(
                      this.clContext.getContext().getConfiguration())).getRaw();
                final LocalDirAllocator lDirAlloc = 
                    new LocalDirAllocator("mapred.local.dir");

                final int mySpillNo = MapTask.numSpills.getAndIncrement();
                final SpillRecord spillRec = new SpillRecord(partitions, mySpillNo);
                final Path filename = lDirAlloc.getLocalPathForWrite(
                    TaskTracker.OUTPUT + "/spill"+mySpillNo+".out",
                    this.clContext.getContext().getConfiguration());
                out = rfs.create(filename);

                final CombineOutputCollector combineCollector;
                final CombinerRunner combinerRunner = CombinerRunner.create(
                    ((org.apache.hadoop.mapred.JobConf)this.clContext.getContext().getConfiguration()),
                    (org.apache.hadoop.mapred.TaskAttemptID)this.clContext.getContext().getTaskAttemptID(),
                    (org.apache.hadoop.mapred.Counters.Counter)combineInputCounter, null, null, null);
                if (combinerRunner != null) {
                    combineCollector = new CombineOutputCollector(
                        (org.apache.hadoop.mapred.Counters.Counter)combineOutputCounter,
                        new Progressable() {
                            @Override
                            public void progress() {
                                clContext.getContext().getReporter().progress();
                            }
                        },
                        this.clContext.getContext().getConfiguration());
                } else {
                    combineCollector = null;
                }

                final SortedWriter writer = new SortedWriter(
                        this.clContext.getContext().getConfiguration(), out,
                        toWrite.peekFirst().buffer().getOutputKeyClass(),
                        toWrite.peekFirst().buffer().getOutputValClass(), null,
                        (org.apache.hadoop.mapred.Counters.Counter)spilledRecordsCounter,
                        ((org.apache.hadoop.mapred.JobConf)this.clContext.getContext().getConfiguration()).getOutputKeyComparator(),
                        true, mySpillNo, true);

                // LOG:DIAGNOSTIC
                // log("    At end, "+toWrite.size()+" output buffers remaining to write");
                if (!toWrite.isEmpty()) {
                    // Just try and fill the spill buffer a little more
                    // doSingleOutputBuffer(toWrite.removeFirst());

                    final OutputBufferSoFar soFar = toWrite.getFirst();
                    final HadoopCLOutputBuffer buffer = soFar.buffer();
                    HadoopCLKeyValueIterator iter = buffer.getKeyValueIterator(toWrite, partitions);

                    if (this.clContext.getCombinerKernel() == null) {
                        while (iter.next()) {
                            writer.append(iter.getKey(), iter.getValue());
                        }
                    } else {
                        combineCollector.setWriter(writer);
                        combinerRunner.combine(iter, combineCollector);
                    }
                    // LOG:DIAGNOSTIC
                    // log("      Finished combine from mapper on "+soFar.buffer().id);
                }

                writer.close();

                HashMap<Integer, Long> partitionSegmentStarts =
                    writer.getPartitionSegmentStarts();
                HashMap<Integer, Long> partitionRawLengths =
                    writer.getPartitionRawLengths();
                HashMap<Integer, Long> partitionCompressedLengths =
                    writer.getPartitionCompressedLengths();

                for (int part = 0; part < partitions; part++) {
                    final IndexRecord rec = new IndexRecord();

                    rec.startOffset = partitionSegmentStarts.get(part);
                    rec.rawLength = partitionRawLengths.get(part);
                    rec.partLength = partitionCompressedLengths.get(part);
                    spillRec.putIndex(rec, part);
                }

                synchronized (MapTask.indexCacheList) {
                    MapTask.indexCacheList.put(spillRec.getSpillNo(), spillRec);
                    MapTask.totalIndexCacheMemory +=
                        spillRec.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
                }

                for (OutputBufferSoFar soFar : toWrite) {
                    freeOutputBuffers.free(soFar.buffer());
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (out != null) out.close();
                } catch (IOException io) {
                    throw new RuntimeException(io);
                }
            }
        } else {
            // LOG:DIAGNOSTIC
            // log("    At end, "+toWrite.size()+" output buffers remaining to write");
            while (!toWrite.isEmpty()) {
                boolean forwardProgress =
                    doSingleOutputBuffer(toWrite.removeFirst());

                if (!forwardProgress) {
                    waitForMoreWork();
                }
            }
        }
    }

    @Override
    public void run() {
        // LOG:PROFILE
        // OpenCLDriver.logger.log("Preallocating kernels", this.clContext);
        for (int i = 0; i < this.clContext.getNKernels(); i++) {
            HadoopCLKernel newKernel;
            try {
                newKernel = this.clContext.thisKernelConstructor.newInstance(
                        this.clContext, i);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            newKernel.doEntrypointInit(this.clContext.getDevice(),
                this.clContext.getContext().getTaskAttemptID().getTaskID().getId(),
                this.clContext.getContext().getTaskAttemptID().getId());
            this.freeKernels.add(newKernel);
        }

        // this.freeKernels.preallocateKernels();
        OpenCLDevice combinerDevice;
        if (this.clContext.isMapper() && this.clContext.jobHasCombiner() &&
                (combinerDevice = this.clContext.getCombinerDevice()) != null) {
            KernelRunner.doKernelAndArgLinesPrealloc(
                this.clContext.getCombinerKernel(),
                // this.clContext.newCombinerKernelObject(),
                Kernel.TaskType.COMBINER, this.clContext.nCombinerKernels(),
                combinerDevice);
        }
        // LOG:PROFILE
        // OpenCLDriver.logger.log("Done reallocating kernels", this.clContext);

        /*
         * I removed the condition !toWrite.isEmpty() because I'd rather
         * exit the loop and then just loop on toWrite after setting
         * usingOpencl to false (this should be more efficient so we're
         * not just constantly throwing exceptions
         */
        while (!mainDone || !toRunPrivate.isEmpty() /* ||
                !toCopyFromOpenCL.isEmpty() || kernelsActive.get() > 0 */ ) {

            boolean forwardProgress = false;

            /*
             * Input Buffer Handling
             */
            forwardProgress |= doInputBuffers();

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

            if (!forwardProgress) {
                waitForMoreWork();
            }
        }

        while (kernelsActive.get() > 0 || !toCopyFromOpenCL.isEmpty()) {
            if (this.freeOutputBuffers.nAvailable() == 0) {
                spillAll();
            }

            HadoopCLKernel kernel;
            synchronized (somethingHappenedLocal) {
                while (toCopyFromOpenCL.isEmpty()) {
                    try {
                        somethingHappenedLocal.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                kernel = toCopyFromOpenCL.poll();
            }
            // must succeed due to spillAll above
            HadoopCLOutputBuffer output = freeOutputBuffers.alloc().obj();
            handleOpenCLCopy(kernel, output);
        }

        if (!toWrite.isEmpty()) {
            spillAll();
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
        sb.append("  toRunPrivate: [ ");
        for (HadoopCLInputBuffer b : this.toRunPrivate) {
            sb.append(b.id);
            sb.append(" ");
        }
        sb.append("]\n");
        // sb.append("  toWrite: [ ");
        // for (OutputBufferSoFar b : this.toWrite) {
        //     sb.append(b.buffer().id);
        //     sb.append(" ");
        // }
        // sb.append("]\n");
        sb.append("  freeKernels: ");
        sb.append(this.freeKernels.toString());
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
}
