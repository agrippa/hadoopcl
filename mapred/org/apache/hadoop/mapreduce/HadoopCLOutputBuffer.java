package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public abstract class HadoopCLOutputBuffer extends HadoopCLBuffer {
    public int[] memIncr;
    protected int[] outputIterMarkers;
    protected final HashSet<Integer> itersFinished = new HashSet<Integer>();

    public abstract void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext);
    public abstract int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar)
        throws IOException, InterruptedException;
    public abstract void copyOverFromInput(HadoopCLInputBuffer inputBuffer);
    public abstract void constructIterSet();

    @Override
    public long space() {
        return super.space() + (4 * memIncr.length);
    }
}
