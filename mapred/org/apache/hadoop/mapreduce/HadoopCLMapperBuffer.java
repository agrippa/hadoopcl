
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.lang.InterruptedException;
import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;
import java.util.HashMap;

public abstract class HadoopCLMapperBuffer extends HadoopCLBuffer {

    protected int nPairs;
    protected int[] nWrites;
    private int capacity;
    protected int isGPU;
    protected boolean enableStriding;
	
	public abstract HashMap getKeyCounts();
	public abstract void populate(Object genericReducerKeys, Object genericReducerValues, 
			int[] keyIndex);

    public void baseInit(HadoopOpenCLContext clContext) {
        this.clContext = clContext;
        this.nWrites = new int[this.clContext.getBufferSize()];
        this.nPairs = 0;
        this.capacity = this.clContext.getBufferSize();
        this.isGPU = this.clContext.isGPU();
        this.memIncr = new int[1];
        this.resetProfile();
        this.enableStriding = this.clContext.runningOnGPU();
    }

    public boolean doIntermediateReduce() {
        return false;
    }


    public int capacity() {
        return capacity;
    }

    public int nContents() {
        return this.nPairs;
    }

    public boolean hasWork() {
        return this.nPairs > 0;
    }

    public boolean completedAll() {
        for(int i = 0; i < nWrites.length; i++) {
            if(nWrites[i] == -1) return false;
        }
        return true;
    }

    public void addKeyAndValue(TaskInputOutputContext context) throws IOException, InterruptedException {
        addTypedKey(((Context)context).getCurrentKey());
        addTypedValue(((Context)context).getCurrentValue());
        this.nPairs++;
    }
}
