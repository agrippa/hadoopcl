package org.apache.hadoop.mapreduce;

import java.util.Deque;
import org.apache.hadoop.mapreduce.BufferRunner.OutputBufferSoFar;
import java.util.Stack;
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
import java.util.TreeSet;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.io.DataOutputStream;
import org.apache.hadoop.io.ReadArrayUtils;
import org.apache.hadoop.mapreduce.Reducer.Context;

public abstract class IntIntIntIntHadoopCLReducerKernel extends HadoopCLReducerKernel {
    public int outputLength;
    protected HadoopCLResizableIntArray bufferedVals = null;


    public  int[] inputKeys;
    public  int[] inputVals;
    public  int[] outputKeys;
    public  int[] outputVals;

    protected abstract void reduce(int key, HadoopCLIntValueIterator valIter);

    public void postKernelSetup(int[] setOutputkeys, int[] setOutputvals, int[] setMemIncr, int[] setNWrites, int[] outputIterMarkers) {
        this.outputKeys = setOutputkeys;
        this.outputVals = setOutputvals;

        this.memIncr = setMemIncr;
        this.nWrites = setNWrites;

        this.outputIterMarkers = outputIterMarkers;
    }

    public void preKernelSetup(int[] setInputkeys, int[] setInputvals, int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals) {
        this.inputKeys = setInputkeys;
        this.inputVals = setInputvals;
        this.outputKeys = null;
        this.outputVals = null;
        this.input_keyIndex = setKeyIndex;
        this.nWrites = setNWrites;
        this.nKeys = setNKeys;
        this.nVals = setNVals;

        this.memIncr = null;

        this.outputLength = this.getArrayLength("outputVals");
        this.outputIterMarkers = null;
    }

    public IntIntIntIntHadoopCLReducerKernel() {
        super(null, null);
        throw new UnsupportedOperationException();
    }
    public IntIntIntIntHadoopCLReducerKernel(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.setStrided(false);

        this.arrayLengths.put("outputIterMarkers", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("memIncr", 1);
        this.arrayLengths.put("outputKeys", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputVals", this.clContext.getOutputBufferSize());
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntIntHadoopCLInputReducerBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntIntHadoopCLOutputReducerBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer) {
        IntIntHadoopCLInputReducerBuffer inputBuffer = (IntIntHadoopCLInputReducerBuffer)genericInputBuffer;
        this.preKernelSetup(inputBuffer.inputKeys, inputBuffer.inputVals, inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals);
    }

    @Override
    public void prepareForRead(HadoopCLOutputBuffer genericOutputBuffer) {
        IntIntHadoopCLOutputReducerBuffer outputBuffer = (IntIntHadoopCLOutputReducerBuffer)genericOutputBuffer;
        this.postKernelSetup(outputBuffer.outputKeys, outputBuffer.outputVals, outputBuffer.memIncr, outputBuffer.nWrites, outputBuffer.outputIterMarkers);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return true;
    }

    private final IntWritable keyObj = new IntWritable();
    private final IntWritable valObj = new IntWritable();

    protected boolean write(int key, int val) {
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
    public IHadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException {
        Context ctx = (Context)context;
        if (this.clContext.doHighLevelProfiling()) {
            this.javaProfile = new HadoopCLAccumulatedProfile();
        } else {
            this.javaProfile = new HadoopCLEmptyAccumulatedProfile();
        }
        this.javaProfile.startOverall();
        this.bufferedVals = new HadoopCLResizableIntArray();
        while(ctx.nextKeyValue()) {
            this.javaProfile.startRead();
            IntWritable key = (IntWritable)ctx.getCurrentKey();
            Iterable<IntWritable> values = (Iterable<IntWritable>)ctx.getValues();
            this.bufferedVals.reset();
            for(IntWritable v : values) {
                this.bufferedVals.add(v.get());
            }
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            reduce(key.get(),             new HadoopCLIntValueIterator((int[])this.bufferedVals.getArray(), this.bufferedVals.size()));
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

