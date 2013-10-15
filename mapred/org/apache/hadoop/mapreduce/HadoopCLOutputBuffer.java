package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public abstract class HadoopCLOutputBuffer extends HadoopCLBuffer {
    public int[] memIncr;

    public abstract void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext);
    public abstract int putOutputsIntoHadoop(TaskInputOutputContext context, ReentrantLock spillLock, int soFar)
        throws IOException, InterruptedException;

    public abstract void copyOverFromInput(HadoopCLInputBuffer inputBuffer);

    @Override
    public long space() {
        return super.space() + (4 * memIncr.length);
    }
}
