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

public class IntFloatHadoopCLInputReducerBuffer extends HadoopCLInputReducerBuffer {
    public int[] inputKeys;
    public float[] inputVals;
    protected int outputsPerInput;


    @Override
    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);

        int inputValPerInputKey = this.getInputValPerInputKey();
        this.tempBuffer1 = new HadoopCLResizableFloatArray();

        inputKeys = new int[this.clContext.getBufferSize()];
        inputVals = new float[this.clContext.getBufferSize() * inputValPerInputKey];
        this.initialized = true;
    }

    @Override
    public void bufferInputValue(Object obj) {
        FloatWritable actual = (FloatWritable)obj;
        ((HadoopCLResizableFloatArray)this.tempBuffer1).add(actual.get());
    }

    @Override
    public void useBufferedValues() {
        System.arraycopy(this.tempBuffer1.getArray(), 0, this.inputVals, this.nVals, this.tempBuffer1.size());
        this.nVals += this.tempBuffer1.size();
    }
    @Override
    public void addTypedValue(Object val) {
        FloatWritable actual = (FloatWritable)val;
        this.inputVals[this.nVals] = actual.get();
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
        return (this.nKeys == this.inputKeys.length || this.nVals + this.numBufferedValues() > this.inputVals.length);
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
            (inputVals.length * 4);
    }

}

