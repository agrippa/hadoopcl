package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import com.amd.aparapi.Range;
import com.amd.aparapi.Kernel;
import org.apache.hadoop.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IntUPairHadoopCLInputReducerBuffer extends HadoopCLInputReducerBuffer {
    public int[] inputKeys;
    public int[] inputValIds;
    public double[] inputVals1;
    public double[] inputVals2;
    protected int outputsPerInput;


    @Override
    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);

        int inputValPerInputKey = this.getInputValPerInputKey();
        this.tempBuffer1 = new HadoopCLResizableIntArray();
        this.tempBuffer2 = new HadoopCLResizableDoubleArray();
        this.tempBuffer3 = new HadoopCLResizableDoubleArray();

        inputKeys = new int[this.clContext.getBufferSize()];
        inputValIds = new int[this.clContext.getBufferSize() * inputValPerInputKey];
        inputVals1 = new double[this.clContext.getBufferSize() * inputValPerInputKey];
        inputVals2 = new double[this.clContext.getBufferSize() * inputValPerInputKey];
        this.initialized = true;
    }

    @Override
    public void bufferInputValue(Object obj) {
        UniquePairWritable actual = (UniquePairWritable)obj;
        ((HadoopCLResizableIntArray)this.tempBuffer1).add(actual.getIVal());
        ((HadoopCLResizableDoubleArray)this.tempBuffer2).add(actual.getVal1());
        ((HadoopCLResizableDoubleArray)this.tempBuffer3).add(actual.getVal2());
    }

    @Override
    public void useBufferedValues() {
        System.arraycopy(this.tempBuffer1.getArray(), 0, this.inputValIds, this.nVals, this.tempBuffer1.size());
        System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputVals1, this.nVals, this.tempBuffer2.size());
        System.arraycopy(this.tempBuffer3.getArray(), 0, this.inputVals2, this.nVals, this.tempBuffer3.size());
        this.nVals += this.tempBuffer1.size();
    }
    @Override
    public void addTypedValue(Object val) {
        UniquePairWritable actual = (UniquePairWritable)val;
        this.inputValIds[this.nVals] = actual.getIVal();
        this.inputVals1[this.nVals] = actual.getVal1();
        this.inputVals2[this.nVals] = actual.getVal2();
    }

    @Override
    public void addTypedKey(Object key) {
        IntWritable actual = (IntWritable)key;
        this.inputKeys[this.nKeys] = actual.get();
    }

    @Override
    public boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {
        Context reduceContext = (Context)context;
        tempBuffer1.reset();
        if(tempBuffer2 != null) tempBuffer2.reset();
        if(tempBuffer3 != null) tempBuffer3.reset();
        for(Object v : reduceContext.getValues()) {
            bufferInputValue(v);
        }
        return (this.nKeys == this.inputKeys.length || this.nVals + this.numBufferedValues() > this.inputVals1.length);
    }

    @Override
    public void reset() {
        this.nKeys = 0;
        this.nVals = 0;
        this.maxInputValsPerInputKey = 0;
    }

    @Override
    public void transferBufferedValues(HadoopCLBuffer buffer) {
        this.tempBuffer1.copyTo(((HadoopCLInputReducerBuffer)buffer).tempBuffer1);
        if(this.tempBuffer2 != null) this.tempBuffer2.copyTo(((HadoopCLInputReducerBuffer)buffer).tempBuffer2);
        if(this.tempBuffer3 != null) this.tempBuffer3.copyTo(((HadoopCLInputReducerBuffer)buffer).tempBuffer3);
    }
    @Override
    public void resetForAnotherAttempt() {
        // NO-OP at the moment, but might be necessary later
    }

    @Override
    public long space() {
        return super.space() + 
            (inputKeys.length * 4) +
            (inputVals1.length * 8) +
            (inputVals2.length * 8) +
            (inputValIds.length * 4);
    }

}

