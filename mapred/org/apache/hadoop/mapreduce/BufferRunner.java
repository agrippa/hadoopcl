package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;

public class BufferRunner implements Runnable {
    private final BufferManager<HadoopCLInputBuffer> freeInputBuffers;
    private final BufferManager<HadoopCLOutputBuffer> freeOutputBuffers;

    private final HadoopCLLimitedQueue<HadoopCLInputBuffer> toRun;
    private final HadoopCLLimitedQueue<HadoopCLOutputBuffer> toWrite;

    private final HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair> running;

    private final LinkedList<HadoopCLKernel> savedKernelObjects;

    private final Class kernelClass;
    private final HadoopOpenCLContext clContext;

    public BufferRunner(Class kernelClass,
            BufferManager<HadoopCLInputBuffer> freeInputBuffers,
            BufferManager<HadoopCLOutputBuffer> freeOutputBuffers,
            HadoopOpenCLContext clContext) {
        this.kernelClass = kernelClass;
        this.freeInputBuffers = freeInputBuffers;
        this.freeOutputBuffers = freeOutputBuffers;

        this.toRun = new HadoopCLLimitedQueue<HadoopCLInputBuffer>();
        this.toWrite = new HadoopCLLimitedQueue<HadoopCLOutputBuffer>();

        this.running = new HashMap<HadoopCLKernel, HadoopCLInputOutputBufferPair>();
        this.savedKernelObjects = new LinkedList<HadoopCLKernel>();

        this.clContext = clContext;
    }

    private HadoopCLKernel newKernelInstance() {
        HadoopCLKernel kernel = null;
        try {
            kernel = (HadoopCLKernel)this.kernelClass.newInstance();
            kernel.init(this.clContext);
            kernel.setGlobals(this.clContext.getGlobalsInd(),
                    this.clContext.getGlobalsVal(),
                    this.clContext.getGlobalIndices(), this.clContext.getNGlobals());
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
        return kernel;
    }

    private List<HadoopCLKernel> getCompleteKernels() {
        List<HadoopCLKernel> complete = new LinkedList<HadoopCLKernel>();
        for (HadoopCLKernel k : this.running.keySet()) {
            /*
            if (k.hasFinished()) {
                complete.add(k);
            }
            */
        }
        return complete;
    }

    private void startKernel(HadoopCLKernel kernel,
            HadoopCLInputBuffer inputBuffer, HadoopCLOutputBuffer outputBuffer,
            boolean firstAttempt) {
        if (firstAttempt) {
            inputBuffer.clearNWrites();
            inputBuffer.getProfile().startKernel();
        }
        kernel.fill(inputBuffer, outputBuffer);
        try {
            kernel.launchKernel();
        } catch(Exception io) {
            throw new RuntimeException(io);
        }
        inputBuffer.getProfile().addKernelAttempt();
    }

    @Override
    public void run() {
        boolean mainDone = false;

        while (!mainDone && !running.isEmpty()) {
            BufferTypeContainer<HadoopCLInputBuffer> inputBufferContainer = toRun.nonBlockingGet();
            if (inputBufferContainer != null) {
                // Have a kernel from main to run
            } else {
                // Try to get an output buffer to dump
                List<HadoopCLKernel> completed = getCompleteKernels();

            }
        }
    }
}
