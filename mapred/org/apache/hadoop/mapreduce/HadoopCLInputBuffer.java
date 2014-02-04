package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;

public abstract class HadoopCLInputBuffer extends HadoopCLBuffer {
    protected boolean doingBulkRead = false;
    protected boolean isFull = false;

    public abstract void init(int pairsPerInput, HadoopOpenCLContext clContext);
    public abstract boolean isFull(TaskInputOutputContext context)
        throws IOException, InterruptedException;
    // public abstract void transferBufferedValues(HadoopCLBuffer buffer);
    // public abstract void resetForAnotherAttempt();
    public abstract void reset();
    public abstract void addKeyAndValue(TaskInputOutputContext context) throws IOException, InterruptedException;
    public abstract boolean hasWork();
    public abstract void addTypedValue(Object val);
    public abstract void addTypedKey(Object key);

    public int bulkFill(HadoopCLDataInput stream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long space() {
        return super.space();
    }

    public void setDoingBulkRead() {
        this.doingBulkRead = true;
    }
}
