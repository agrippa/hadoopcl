package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.mapreduce.Reducer.Context;
import com.amd.aparapi.Range;
import com.amd.aparapi.Kernel;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import java.util.List;
import java.util.ArrayList;

public abstract class HadoopCLInputReducerBuffer extends HadoopCLInputBuffer {
    public final int[] keyIndex;
    public int nKeys;
    public int nVals;
    public final int keyCapacity;
    public final int valCapacity;

    public HadoopCLInputReducerBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.keyIndex = new int[this.clContext.getInputBufferSize()];
        this.nKeys = 0;
        this.nVals = 0;
        this.keyCapacity = this.clContext.getInputBufferSize();
        this.valCapacity = this.clContext.getInputBufferSize() *
            this.clContext.getInputValMultiplier();
    }

    public boolean hasWork() {
        return this.nKeys > 0;
    }

    @Override
    public boolean completedAll() {
        // int count = 0;
        // for (int i = 0; i < this.nKeys; i++) {
        //   if (nWrites[i] == -1) count++;
        // }
        // System.out.println("Did not complete "+count);
        for(int i = 0; i < this.nKeys; i++) {
            if(nWrites[i] == -1) return false;
        }
        return true;
    }

    @Override
    public void addKeyAndValue(TaskInputOutputContext context)
            throws IOException, InterruptedException {
        addTypedKey(((Context)context).getCurrentKey());
        addTypedValue(((Context)context).getCurrentValue());
    }

    @Override
    public long space() {
        return super.space() + (4 * keyIndex.length);
    }
}
