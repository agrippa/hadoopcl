package org.apache.hadoop.mapreduce;

import java.util.List;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import java.util.LinkedList;

public class ToOpenCLThread implements Runnable {
    public static HadoopCLLimitedQueue<HadoopCLInputBuffer> toRun = null;
    private final BufferManager<HadoopCLInputBuffer> processed;
    private final BufferManager<HadoopCLOutputBuffer> written;
    private int countBuffers = 0;

    private final HadoopOpenCLContext clContext;

    public ToOpenCLThread(HadoopCLKernel setKernel,
            BufferManager<HadoopCLInputBuffer> processed,
            BufferManager<HadoopCLOutputBuffer> written,
            HadoopOpenCLContext setCLContext) {
        this.clContext = setCLContext;
        this.processed = processed;
        this.written = written;
    }

    public static void addWorkFromMain(HadoopCLInputBuffer toAdd) {
        toRun.add(toAdd);
    }

    public static HadoopCLInputBuffer getWork() throws InterruptedException {
        HadoopCLInputBuffer work = toRun.blockingGet();
        return work;
    }

    private HadoopCLKernel getKernel(HadoopOpenCLContext context, boolean isMapper) {
        if(isMapper) {
            return clContext.getMapperKernel();
        } else {
            return clContext.getReducerKernel();
        }
    }

    @Override
    public void run()  {
        try {
            HadoopCLInputBuffer work = getWork();
            while (work != null) {

                boolean isMapper = work instanceof HadoopCLInputMapperBuffer;
                HadoopCLKernel kernel = getKernel(this.clContext, isMapper);

                work.getProfile().startKernel();
                BufferManager.BufferTypeAlloc<HadoopCLOutputBuffer> newOutputBufferContainer = this.written.alloc();

                HadoopCLOutputBuffer newOutputBuffer = newOutputBufferContainer.obj();
                if (newOutputBufferContainer.isFresh()) {
                    newOutputBuffer.initBeforeKernel(kernel.getOutputPairsPerInput(),
                            this.clContext);
                }
                kernel.fill(work, newOutputBuffer);
                kernel.launchKernel();
                work.getProfile().addKernelAttempt();
                boolean completedAll = work.completedAll();
                if (!completedAll) {
                    work.resetForAnotherAttempt();
                }
                newOutputBuffer.copyOverFromInput(work);
                ToHadoopThread.addWork(newOutputBuffer);

                while(!completedAll) {
                    newOutputBufferContainer = this.written.alloc();
                    if (newOutputBufferContainer.isFresh()) {
                        newOutputBuffer.initBeforeKernel(kernel.getOutputPairsPerInput(),
                            this.clContext);
                    }
                    kernel.fill(work, newOutputBuffer);
                    kernel.launchKernel();
                    work.getProfile().addKernelAttempt();
                    completedAll = work.completedAll();
                    if (!completedAll) {
                        work.resetForAnotherAttempt();
                    }

                    newOutputBuffer.copyOverFromInput(work);
                    ToHadoopThread.addWork(newOutputBuffer);
                }

                this.written.free(newOutputBuffer);

                work.getProfile().stopKernel();
                processed.free(work);

                work = getWork();
            }
            ToHadoopThread.addWork(null);
        } catch(Exception e) {
            // not really sure what can be done here...
        }
    }
}
