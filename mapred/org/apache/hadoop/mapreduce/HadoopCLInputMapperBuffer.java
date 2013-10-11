package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.lang.InterruptedException;
import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;
import java.util.HashMap;

public abstract class HadoopCLInputMapperBuffer extends HadoopCLInputBuffer {
    public int nPairs;
    private int capacity;
    protected boolean enableStriding;

    public void baseInit(HadoopOpenCLContext clContext) {
        this.clContext = clContext;
        this.nWrites = new int[this.clContext.getBufferSize()];
        this.nPairs = 0;
        this.capacity = this.clContext.getBufferSize();
        this.isGPU = this.clContext.isGPU();
        this.enableStriding = this.clContext.runningOnGPU();
    }

    public int capacity() {
        return capacity;
    }

    public boolean hasWork() {
        return this.nPairs > 0;
    }

    public boolean completedAll() {
        for(int i = 0; i < this.nPairs; i++) {
            if(nWrites[i] == -1) return false;
        }
        return true;
    }

    public void addKeyAndValue(TaskInputOutputContext context) throws IOException, InterruptedException {
        addTypedKey(((Context)context).getCurrentKey());
        addTypedValue(((Context)context).getCurrentValue());
        this.nPairs++;
    }

    @Override
    public long space() {
        return super.space();
    }
}
