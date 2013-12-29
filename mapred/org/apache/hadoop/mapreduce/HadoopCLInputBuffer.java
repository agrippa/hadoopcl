package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;

public abstract class HadoopCLInputBuffer extends HadoopCLBuffer {
    public abstract void init(int pairsPerInput, HadoopOpenCLContext clContext);
    public abstract boolean isFull(TaskInputOutputContext context)
        throws IOException, InterruptedException;
    public abstract void transferBufferedValues(HadoopCLBuffer buffer);
    public abstract void resetForAnotherAttempt();
    public abstract void reset();
    public abstract void addKeyAndValue(TaskInputOutputContext context) throws IOException, InterruptedException;
    public abstract boolean hasWork();
    public abstract void addTypedValue(Object val);
    public abstract void addTypedKey(Object key);

    @Override
    public long space() {
        return super.space();
    }
}
