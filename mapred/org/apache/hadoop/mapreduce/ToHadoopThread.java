package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import java.util.LinkedList;

public class ToHadoopThread implements Runnable {
    private final HadoopOpenCLContext clContext;
    public static LinkedList<HadoopCLBuffer> toWrite = null;
    public static LinkedList<HadoopCLBuffer> written = null;
    private boolean doIntermediateReduction;

    public ToHadoopThread(HadoopOpenCLContext setCLContext,
            HadoopCLKernel kernel) {
        this.clContext = setCLContext;

        if(kernel instanceof HadoopCLMapperKernel && kernel.doIntermediateReduction()) {
            try {
                Class reducerClass = clContext.getContext().getOCLReducerClass();
                HadoopCLReducerKernel reducerKernel = (HadoopCLReducerKernel)reducerClass.newInstance();
                HadoopCLReducerBuffer reducerBuffer = (HadoopCLReducerBuffer)reducerKernel.getBufferClass().newInstance();
                if(reducerBuffer != null && reducerBuffer.equalInputOutputTypes()) {
                    this.doIntermediateReduction = true;
                } else {
                    this.doIntermediateReduction = false;
                }
                //this.reducerBuffer.init(reducerKernel.getOutputPairsPerInput(), clContext, hadoopContext);
            } catch(Exception ex) {
                this.doIntermediateReduction = false;
            }
        } else {
            this.doIntermediateReduction = false;
        }
    }

    public static void addWork(HadoopCLBuffer toAdd) {
        synchronized(toWrite) {
            toWrite.add(toAdd);
            toWrite.notify();
        }
    }

    public static HadoopCLBuffer getWork() throws InterruptedException {
        HadoopCLBuffer work = null;
        synchronized(toWrite) {
            while(toWrite.isEmpty()) {
                toWrite.wait();
            }
            work = toWrite.poll();
        }
        return work;
    }

    @Override
    public void run()  {
        try {
            while(true) {
                HadoopCLBuffer work = getWork();
                if(work == null) {
                    break;
                } else {
                    work.getProfile().startWrite();
                    HadoopCLReducerBuffer buffer = work.putOutputsIntoHadoop(clContext.getContext(), this.doIntermediateReduction);
                    work.getProfile().stopWrite();
                    ToOpenCLThread.addWorkFromHadoop(buffer);

                    if(work.keep()) {
                        synchronized(written) {
                            written.add(work);
                        }
                    }
                }

            }
        } catch(Exception e) {
            // not really sure what can be done here...
        }
    }
}
