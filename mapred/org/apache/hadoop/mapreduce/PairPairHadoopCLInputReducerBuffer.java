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

public class PairPairHadoopCLInputReducerBuffer extends HadoopCLInputReducerBuffer {
    public double[] inputKeys1;
    public double[] inputKeys2;
    public double[] inputVals1;
    public double[] inputVals2;
    protected int outputsPerInput;


    @Override
    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);

        int inputValPerInputKey = this.getInputValPerInputKey();
        this.tempBuffer1 = new HadoopCLResizableDoubleArray();
        this.tempBuffer2 = new HadoopCLResizableDoubleArray();

        inputKeys1 = new double[this.clContext.getBufferSize()];
        inputKeys2 = new double[this.clContext.getBufferSize()];
        inputVals1 = new double[this.clContext.getBufferSize() * inputValPerInputKey];
        inputVals2 = new double[this.clContext.getBufferSize() * inputValPerInputKey];
        this.initialized = true;
    }

    @Override
    public void bufferInputValue(Object obj) {
        PairWritable actual = (PairWritable)obj;
        ((HadoopCLResizableDoubleArray)this.tempBuffer1).add(actual.getVal1());
        ((HadoopCLResizableDoubleArray)this.tempBuffer2).add(actual.getVal2());
    }

    @Override
    public void useBufferedValues() {
        System.arraycopy(this.tempBuffer1.getArray(), 0, this.inputVals1, this.nVals, this.tempBuffer1.size());
        System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputVals2, this.nVals, this.tempBuffer2.size());
        this.nVals += this.tempBuffer1.size();
    }
    @Override
    public void addTypedValue(Object val) {
        PairWritable actual = (PairWritable)val;
        this.inputVals1[this.nVals] = actual.getVal1();
        this.inputVals2[this.nVals] = actual.getVal2();
    }

    @Override
    public void addTypedKey(Object key) {
        PairWritable actual = (PairWritable)key;
        this.inputKeys1[this.nKeys] = actual.getVal1();
        this.inputKeys2[this.nKeys] = actual.getVal2();
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
        return (this.nKeys == this.inputKeys1.length || this.nVals + this.numBufferedValues() > this.inputVals1.length);
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
            (inputKeys1.length * 8) +
            (inputKeys2.length * 8) +
            (inputVals1.length * 8) +
            (inputVals2.length * 8);
    }

}

