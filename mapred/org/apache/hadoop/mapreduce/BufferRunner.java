package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;

public class BufferRunner implements Runnable {
    private final List<HadoopCLBuffer.Profile> profiles;
    private final BufferManager<HadoopCLInputBuffer> freeInputBuffers;
    private final BufferManager<HadoopCLOutputBuffer> freeOutputBuffers; // exclusive

    private final HadoopCLLimitedQueue<HadoopCLInputBuffer> toRun;
    private final HadoopCLLimitedQueue<HadoopCLInputBuffer> toRunPrivate; // exclusive
    private final LinkedList<HadoopCLOutputBuffer> toWrite; // exclusive

    // exclusive
    private final HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair> running;

    private final LinkedList<HadoopCLKernel> savedKernelObjects;

    private final Class kernelClass;
    private final HadoopOpenCLContext clContext;

    private int nKernelInstances = 0;

    public BufferRunner(Class kernelClass,
            BufferManager<HadoopCLInputBuffer> freeInputBuffers,
            BufferManager<HadoopCLOutputBuffer> freeOutputBuffers,
            HadoopOpenCLContext clContext) {
        this.kernelClass = kernelClass;
        this.freeInputBuffers = freeInputBuffers;
        this.freeOutputBuffers = freeOutputBuffers;

        this.toRun = new HadoopCLLimitedQueue<HadoopCLInputBuffer>();
        this.toRunPrivate = new HadoopCLLimitedQueue<HadoopCLInputBuffer>();
        this.toWrite = new LinkedList<HadoopCLOutputBuffer>();

        this.running = new HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair>();
        this.savedKernelObjects = new LinkedList<HadoopCLKernel>();

        this.clContext = clContext;
        this.profiles = new LinkedList<HadoopCLBuffer.Profile>();
    }

    public List<HadoopCLBuffer.Profile> profiles() {
        return this.profiles;
    }

    public void addWork(HadoopCLInputBuffer input) {
        // possible if getting DONE signal from main
        if (input != null) input.clearNWrites();
        this.toRun.add(input);
    }

    private HadoopCLKernel newKernelInstance() {
        HadoopCLKernel kernel = null;
        if (!savedKernelObjects.isEmpty()) {
            kernel = savedKernelObjects.poll();
        } else {
            try {
                kernel = (HadoopCLKernel)this.kernelClass.newInstance();
                kernel.init(this.clContext);
                kernel.setGlobals(this.clContext.getGlobalsInd(),
                        this.clContext.getGlobalsVal(),
                        this.clContext.getGlobalIndices(), this.clContext.getNGlobals());
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            }
            nKernelInstances++;
        }
        return kernel;
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
            System.err.println("  Successfully started with "+nKernelInstances+" kernel instances");
        } else {
            System.err.println("  Failed starting kernel due to OpenCL error");
        }
        return success;
    }

    @Override
    public void run() {
        boolean mainDone = false;

        while (!mainDone || !running.isEmpty() ||
                !toRunPrivate.isEmpty() || !toWrite.isEmpty()) {

            BufferTypeContainer<HadoopCLInputBuffer> inputBufferContainer = 
                toRun.nonBlockingGet();
            if (inputBufferContainer == null) {
                // try again for any retry input buffers
                inputBufferContainer = toRunPrivate.nonBlockingGet();
            }

            // Special test for DONE signal from main thread
            if (inputBufferContainer != null && inputBufferContainer.get() == null) {
                mainDone = true;
                continue;
            }

            if (inputBufferContainer != null) {
                // Have a kernel from main to run
                HadoopCLKernel k = newKernelInstance();
                BufferManager.BufferTypeAlloc<HadoopCLOutputBuffer> outputBufferContainer =
                    freeOutputBuffers.nonBlockingAlloc();
                if (outputBufferContainer != null) {
                    if (outputBufferContainer.obj() == null) System.err.println("A> Got null output buffer");
                    if (outputBufferContainer.isFresh()) {
                        outputBufferContainer.obj().initBeforeKernel(
                                k.getOutputPairsPerInput(), this.clContext);
                    }
                    if (!startKernel(k, inputBufferContainer.get(), outputBufferContainer.obj())) {
                        toRunPrivate.add(inputBufferContainer.get());
                        freeOutputBuffers.free(outputBufferContainer.obj());
                        savedKernelObjects.add(k);
                    }
                    continue; // one operation per iteration
                } else {
                    toRunPrivate.add(inputBufferContainer.get());
                    savedKernelObjects.add(k);
                }
            } 
            List<HadoopCLKernel> completed = getCompleteKernels();
            if (!completed.isEmpty()) {
                // Try to either re-run incomplete kernels, or just
                // set the output buffers up for dumping
                boolean doContinue = false; // we only want to continue if we actually made progress on this iter
                for (HadoopCLKernel k : completed) {
                    HadoopCLInputOutputBufferPair pair = running.remove(k);
                    HadoopCLInputBuffer input = pair.inputBuffer();
                    HadoopCLOutputBuffer output = pair.outputBuffer();

                    int errCode = k.waitFor();
                    // System.err.println("errCode="+errCode);
                    input.getProfile().stopKernel();

                    // if (errCode != 0) {
                    //     System.err.println("  Kernel failed during execution with error code = "+errCode);
                    //     toRunPrivate.add(input);
                    //     freeOutputBuffers.free(output);
                    //     savedKernelObjects.add(k);
                    // } else {
                        output.copyOverFromInput(input);
                        toWrite.add(output);
                        if (input.completedAll()) {
                            profiles.add(input.getProfile());
                            freeInputBuffers.free(input);
                            savedKernelObjects.add(k);
                        } else {
                            BufferManager.BufferTypeAlloc<HadoopCLOutputBuffer> outputBufferContainer =
                                freeOutputBuffers.nonBlockingAlloc();
                            if (outputBufferContainer != null) {
                                if (outputBufferContainer.obj() == null) System.err.println("B> Got null output buffer");
                                if (outputBufferContainer.isFresh()) {
                                    outputBufferContainer.obj().initBeforeKernel(k.getOutputPairsPerInput(),
                                            this.clContext);
                                }
                                if (!startKernel(k, input, outputBufferContainer.obj())) {
                                    toRunPrivate.add(input);
                                    freeOutputBuffers.free(outputBufferContainer.obj());
                                    savedKernelObjects.add(k);
                                    // Continue if we've just created work early in
                                    //   the pipeline.
                                    // In this case, we've created work because we
                                    //   failed to launch a kernel.
                                    // This usually happens because we're out of GPU
                                    //   memory, so continuing may not be the right
                                    //   thing here.
                                    doContinue = true; // only continue if we've just created work early in the pipeline
                                }
                            } else {
                                toRunPrivate.add(input);
                                savedKernelObjects.add(k);
                            }
                        }
                    // }
                }
                if (doContinue) continue;
            }

            HadoopCLOutputBuffer outputBuffer = toWrite.poll();
            if (outputBuffer != null) {
                try {
                    outputBuffer.getProfile().startWrite();
                    outputBuffer.putOutputsIntoHadoop(clContext.getContext());
                    outputBuffer.getProfile().stopWrite();
                } catch(Exception ex) {
                    throw new RuntimeException(ex);
                }
                this.freeOutputBuffers.free(outputBuffer);
            }
        }
    }
}
