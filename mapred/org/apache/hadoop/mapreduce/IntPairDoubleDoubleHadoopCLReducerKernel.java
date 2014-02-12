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

public abstract class IntPairDoubleDoubleHadoopCLReducerKernel extends HadoopCLReducerKernel {
    public int outputLength;
    protected HadoopCLResizableDoubleArray bufferedVal1 = null;
    protected HadoopCLResizableDoubleArray bufferedVal2 = null;


    public  int[] inputKeys;
    public  double[] inputVals1;
    public  double[] inputVals2;
    public  double[] outputKeys;
    public  double[] outputVals;

    protected abstract void reduce(int key, HadoopCLPairValueIterator valIter);

    public void postKernelSetup(double[] setOutputkeys, double[] setOutputvals, int[] setMemIncr, int[] setNWrites, int[] outputIterMarkers) {
        this.outputKeys = setOutputkeys;
        this.outputVals = setOutputvals;

        this.memIncr = setMemIncr;
        this.nWrites = setNWrites;

        this.outputIterMarkers = outputIterMarkers;
    }

    public void preKernelSetup(int[] setInputkeys, double[] setInputvals1, double[] setInputvals2, int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals) {
        this.inputKeys = setInputkeys;
        this.inputVals1 = setInputvals1;
        this.inputVals2 = setInputvals2;
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

    public IntPairDoubleDoubleHadoopCLReducerKernel() {
        super(null, null);
        throw new UnsupportedOperationException();
    }
    public IntPairDoubleDoubleHadoopCLReducerKernel(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.setStrided(false);

        this.arrayLengths.put("outputIterMarkers", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("memIncr", 1);
        this.arrayLengths.put("outputKeys", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputVals", this.clContext.getOutputBufferSize());
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntPairHadoopCLInputReducerBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return DoubleDoubleHadoopCLOutputReducerBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer) {
        IntPairHadoopCLInputReducerBuffer inputBuffer = (IntPairHadoopCLInputReducerBuffer)genericInputBuffer;
        this.preKernelSetup(inputBuffer.inputKeys, inputBuffer.inputVals1, inputBuffer.inputVals2, inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals);
    }

    @Override
    public void prepareForRead(HadoopCLOutputBuffer genericOutputBuffer) {
        DoubleDoubleHadoopCLOutputReducerBuffer outputBuffer = (DoubleDoubleHadoopCLOutputReducerBuffer)genericOutputBuffer;
        this.postKernelSetup(outputBuffer.outputKeys, outputBuffer.outputVals, outputBuffer.memIncr, outputBuffer.nWrites, outputBuffer.outputIterMarkers);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return false;
    }

    private final DoubleWritable keyObj = new DoubleWritable();
    private final DoubleWritable valObj = new DoubleWritable();

    protected boolean write(double key, double val) {
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
        this.bufferedVal1 = new HadoopCLResizableDoubleArray();
        this.bufferedVal2 = new HadoopCLResizableDoubleArray();
        while(ctx.nextKeyValue()) {
            this.javaProfile.startRead();
            IntWritable key = (IntWritable)ctx.getCurrentKey();
            Iterable<PairWritable> values = (Iterable<PairWritable>)ctx.getValues();
            this.bufferedVal1.reset();
            this.bufferedVal2.reset();
            for(PairWritable v : values) {
                this.bufferedVal1.add(v.getVal1());
                this.bufferedVal2.add(v.getVal2());
            }
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            reduce(key.get(),             new HadoopCLPairValueIterator(
                       (double[])this.bufferedVal1.getArray(),
                       (double[])this.bufferedVal2.getArray(),
                       this.bufferedVal1.size()));
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

