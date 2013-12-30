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
import org.apache.hadoop.io.HadoopCLResizableArray;
import java.util.List;
import java.util.ArrayList;

public abstract class HadoopCLInputReducerBuffer extends HadoopCLInputBuffer {
    public int[] keyIndex;
    public int nKeys;
    public int nVals;

    protected HadoopCLResizableArray tempBuffer1 = null;
    protected HadoopCLResizableArray tempBuffer2 = null;
    protected HadoopCLResizableArray tempBuffer3 = null;

    public int maxInputValsPerInputKey;

    protected abstract void bufferInputValue(Object obj);
    protected abstract void useBufferedValues();

    public int numBufferedValues() {
        return tempBuffer1.size();
    }

    public void baseInit(HadoopOpenCLContext clContext) {
        int valuesPerKeyGuess = 16;
        this.clContext = clContext;
        this.keyIndex = new int[this.clContext.getInputBufferSize()];
        this.nWrites = new int[this.clContext.getInputBufferSize()];
        this.nKeys = 0;
        this.nVals = 0;
        this.isGPU = this.clContext.isGPU();
        this.maxInputValsPerInputKey = 0;
    }

    public int getInputValPerInputKey() {
        return 16;
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
    public void addKeyAndValue(TaskInputOutputContext context) throws IOException, InterruptedException {
        addTypedKey(((Context)context).getCurrentKey());
        this.keyIndex[this.nKeys] = this.nVals;
        this.nKeys = this.nKeys + 1;

        if(this.numBufferedValues() > this.maxInputValsPerInputKey) {
            this.maxInputValsPerInputKey = this.numBufferedValues();
        }

        this.useBufferedValues();

        this.tempBuffer1.reset();
        if(this.tempBuffer2 != null) tempBuffer2.reset();
        if(this.tempBuffer3 != null) tempBuffer3.reset();
    }

    @Override
    public long space() {
        return super.space() + (4 * keyIndex.length) + 
            (tempBuffer1 == null ? 0 : tempBuffer1.space()) +
            (tempBuffer2 == null ? 0 : tempBuffer2.space()) +
            (tempBuffer3 == null ? 0 : tempBuffer3.space());
    }
}
