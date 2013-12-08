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

public abstract class IntDoubleIntDoubleHadoopCLReducerKernel extends HadoopCLReducerKernel {
    protected int outputLength;
    protected HadoopCLResizableDoubleArray bufferedVals = null;


    public int[] inputKeys;
    public double[] inputVals;
    public int[] outputKeys;
    public double[] outputVals;
    final private IntWritable keyObj = new IntWritable();
    final private DoubleWritable valObj = new DoubleWritable();

    protected abstract void reduce(int key, HadoopCLDoubleValueIterator valIter);

    public void setup(int[] setInputkeys, double[] setInputvals, int[] setOutputkeys, double[] setOutputvals, int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals, int[] setMemIncr, int setOutputsPerInput) {
        this.inputKeys = setInputkeys;
        this.inputVals = setInputvals;
        this.outputKeys = setOutputkeys;
        this.outputVals = setOutputvals;
        this.input_keyIndex = setKeyIndex;
        this.nWrites = setNWrites;
        this.nKeys = setNKeys;
        this.nVals = setNVals;

        this.memIncr = setMemIncr;
        this.memIncr[0] = 0;
        this.outputsPerInput = setOutputsPerInput;

        this.outputLength = outputVals.length;
    }

    @Override
    public void init(HadoopOpenCLContext clContext) {
        baseInit(clContext);
        this.setStrided(false);
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntDoubleHadoopCLInputReducerBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntDoubleHadoopCLOutputReducerBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer, HadoopCLOutputBuffer genericOutputBuffer) {
        IntDoubleHadoopCLInputReducerBuffer inputBuffer = (IntDoubleHadoopCLInputReducerBuffer)genericInputBuffer;
        IntDoubleHadoopCLOutputReducerBuffer outputBuffer = (IntDoubleHadoopCLOutputReducerBuffer)genericOutputBuffer;
        if(this.outputsPerInput < 0 && (outputBuffer.outputKeys == null || outputBuffer.outputKeys.length < inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey)) {
            outputBuffer.outputKeys = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
            outputBuffer.outputVals = new double[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
        }
        this.setup(inputBuffer.inputKeys, inputBuffer.inputVals, outputBuffer.outputKeys, outputBuffer.outputVals, inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals, outputBuffer.memIncr, this.outputsPerInput);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return true;
    }
    protected boolean write(int key, double val) {
        this.javaProfile.stopKernel();
        this.javaProfile.startWrite();
        keyObj.set(key);
        valObj.set(val);
        try { clContext.getContext().write(keyObj, valObj); } catch(Exception ex) { throw new RuntimeException(ex); }
        this.javaProfile.stopWrite();
        this.javaProfile.startKernel();
        return true;
    }

    @Override
    protected void callReduce(int startOffset, int stopOffset)  {
        reduce(inputKeys[3], null);
    }
    @Override
    public HadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException {
        Context ctx = (Context)context;
        this.javaProfile = new HadoopCLAccumulatedProfile();
        this.javaProfile.startOverall();
        this.bufferedVals = new HadoopCLResizableDoubleArray();
        while(ctx.nextKeyValue()) {
            this.javaProfile.startRead();
            IntWritable key = (IntWritable)ctx.getCurrentKey();
            Iterable<DoubleWritable> values = (Iterable<DoubleWritable>)ctx.getValues();
            this.bufferedVals.reset();
            for(DoubleWritable v : values) {
                this.bufferedVals.add(v.get());
            }
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            reduce(key.get()
, new HadoopCLDoubleValueIterator((double[])this.bufferedVals.getArray(), this.bufferedVals.size()));
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

