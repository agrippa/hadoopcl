package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.util.HashSet;
import java.io.IOException;
import java.lang.InterruptedException;
import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;
import java.util.HashMap;

public abstract class HadoopCLInputMapperBuffer extends HadoopCLInputBuffer {
    public int nPairs;
    public final int capacity;
    public final boolean enableStriding;

    public HadoopCLInputMapperBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.nPairs = 0;
        this.capacity = this.clContext.getInputBufferSize();
        this.enableStriding = this.clContext.runningOnGPU();
        System.err.println("this.enableStriding="+enableStriding);
        System.exit(1);
    }

    public boolean hasWork() {
        return this.nPairs > 0;
    }

    public void addKeyAndValue(TaskInputOutputContext context)
            throws IOException, InterruptedException {
        addTypedKey(((Context)context).getCurrentKey());
        addTypedValue(((Context)context).getCurrentValue());
        this.nPairs++;
        this.isFull |= this.nPairs == this.capacity;
    }

    @Override
    public long space() {
        return super.space();
    }

    @Override
    public final void clearNWrites() {
        for (int i = 0; i < nPairs; i++) {
            this.nWrites[i] = -1;
        }
    }
}
