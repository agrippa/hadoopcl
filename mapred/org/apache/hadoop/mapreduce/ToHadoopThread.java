package org.apache.hadoop.mapreduce;

import java.util.List;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import java.util.LinkedList;

public class ToHadoopThread implements Runnable {
    private final HadoopOpenCLContext clContext;
    public static HadoopCLLimitedQueue<HadoopCLOutputBuffer> toWrite = null;
    private final BufferManager<HadoopCLOutputBuffer> written;

    public ToHadoopThread(HadoopOpenCLContext setCLContext,
            BufferManager<HadoopCLOutputBuffer> written,
            HadoopCLKernel kernel) {
        this.clContext = setCLContext;
        this.written = written;
    }

    public static void addWork(HadoopCLOutputBuffer toAdd) {
        toWrite.add(toAdd);
    }

    public static HadoopCLOutputBuffer getWork() throws InterruptedException {
        return toWrite.blockingGet();
    }

    @Override
    public void run()  {
        try {
            while(true) {
                HadoopCLOutputBuffer work = getWork();
                if(work == null) {
                    break;
                } else {
                    work.getProfile().startWrite();
                    work.putOutputsIntoHadoop(clContext.getContext());
                    work.getProfile().stopWrite();
                    this.written.free(work);
                }

            }
        } catch(Exception e) {
            // not really sure what can be done here...
        }
    }
}
