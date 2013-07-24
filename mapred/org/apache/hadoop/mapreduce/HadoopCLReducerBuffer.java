package org.apache.hadoop.mapreduce;

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

public abstract class HadoopCLReducerBuffer extends HadoopCLBuffer {

    public int[] keyIndex;
    public int[] nWrites;
    public int nKeys;
    public int nVals;

    protected int isGPU;

    protected int keyCapacity;
    protected int valCapacity;

    protected HadoopCLResizableArray tempBuffer1 = null;
    protected HadoopCLResizableArray tempBuffer2 = null;
    protected HadoopCLResizableArray tempBuffer3 = null;

    protected int maxInputValsPerInputKey;

    protected abstract void bufferInputValue(Object obj);
    protected abstract void useBufferedValues();
	public abstract void init(HadoopCLMapperBuffer mapperBuffer);

    public int numBufferedValues() {
        return tempBuffer1.size();
    }

    public void baseInit(HadoopOpenCLContext clContext) {
        int valuesPerKeyGuess = 16;
        this.clContext = clContext;
        this.keyIndex = new int[this.clContext.getBufferSize()];
        this.nWrites = new int[this.clContext.getBufferSize()];
        this.nKeys = 0;
        this.nVals = 0;
        this.keyCapacity = this.clContext.getBufferSize();
        this.valCapacity = this.clContext.getBufferSize() * valuesPerKeyGuess;
        this.isGPU = this.clContext.isGPU();
        this.maxInputValsPerInputKey = 0;
        this.memIncr = new int[1];
    }

    public int getInputValPerInputKey() {
        return 16;
    }

    public int keyCapacity() {
        return this.keyCapacity;
    }

    public int valCapacity() {
        return valCapacity;
    }

    public int nContents() {
        return this.nKeys;
    }

    public boolean hasWork() {
        return this.nKeys > 0;
    }

    public boolean completedAll() {
        for(int i = 0; i < nWrites.length; i++) {
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
}
