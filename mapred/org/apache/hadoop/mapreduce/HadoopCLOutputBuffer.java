package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public abstract class HadoopCLOutputBuffer extends HadoopCLBuffer {
    public int[] memIncr;
    protected int[] outputIterMarkers;
    public HashSet<Integer> itersFinished;

    public abstract void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext);
    public abstract int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar)
        throws IOException, InterruptedException;

    public abstract void copyOverFromKernel(HadoopCLKernel kernel);
    public abstract HashSet<Integer> constructIterSet();

    @Override
    public long space() {
        return super.space() + (4 * memIncr.length);
    }
}
