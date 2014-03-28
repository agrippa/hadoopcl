package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;

public abstract class HadoopCLInputBuffer extends HadoopCLBuffer {
    protected final boolean doingBulkRead;
    protected boolean isFull = false;

    public HadoopCLInputBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.doingBulkRead = clContext.getContext().supportsBulkReads();
    }

    public abstract int getNInputs();
    public abstract boolean isFull(TaskInputOutputContext context)
        throws IOException, InterruptedException;
    public abstract void reset();
    public abstract void addKeyAndValue(TaskInputOutputContext context) throws IOException, InterruptedException;
    public abstract boolean hasWork();
    public abstract void addTypedValue(Object val);
    public abstract void addTypedKey(Object key);

    public abstract int bulkFill(HadoopCLDataInput stream) throws IOException;
    @Override
    public long space() {
        return super.space();
    }

    public abstract void clearNWrites();
}
