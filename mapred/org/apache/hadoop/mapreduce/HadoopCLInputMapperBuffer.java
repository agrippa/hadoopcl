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
    }

    public boolean hasWork() {
        return this.nPairs > 0;
    }

    @Override
    public boolean completedAll() {
        /*
        int count = 0;
        for (int i = 0; i < this.nPairs; i++) {
          if (nWrites[i] == -1) count++;
        }
        System.out.println("Did not complete "+count);
        */
        for(int i = 0; i < this.nPairs; i++) {
            if(nWrites[i] == -1) return false;
        }
        return true;
    }

    public void addKeyAndValue(TaskInputOutputContext context)
            throws IOException, InterruptedException {
        addTypedKey(((Context)context).getCurrentKey());
        addTypedValue(((Context)context).getCurrentValue());
        this.nPairs++;
    }

    @Override
    public long space() {
        return super.space();
    }
}
