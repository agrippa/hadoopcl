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

public abstract class IntUPairIntUPairHadoopCLReducerKernel extends HadoopCLReducerKernel {
    protected int outputLength;
    protected HadoopCLResizableIntArray bufferedValId = null;
    protected HadoopCLResizableDoubleArray bufferedVal1 = null;
    protected HadoopCLResizableDoubleArray bufferedVal2 = null;


    public int[] inputKeys;
    public int[] inputValIds;
    public double[] inputVals1;
    public double[] inputVals2;
    public int[] outputKeys;
    public int[] outputValIds;
    public double[] outputVals1;
    public double[] outputVals2;
    final private IntWritable keyObj = new IntWritable();
    final private UniquePairWritable valObj = new UniquePairWritable();

    protected abstract void reduce(int key, HadoopCLUPairValueIterator valIter);

    public void setup(int[] setInputkeys, int[] setInputvalIds, double[] setInputvals1, double[] setInputvals2, int[] setOutputkeys, int[] setOutputvalIds, double[] setOutputvals1, double[] setOutputvals2, int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals, int[] setMemIncr, int setOutputsPerInput) {
        this.inputKeys = setInputkeys;
        this.inputValIds = setInputvalIds;
        this.inputVals1 = setInputvals1;
        this.inputVals2 = setInputvals2;
        this.outputKeys = setOutputkeys;
        this.outputValIds = setOutputvalIds;
        this.outputVals1 = setOutputvals1;
        this.outputVals2 = setOutputvals2;
        this.input_keyIndex = setKeyIndex;
        this.nWrites = setNWrites;
        this.nKeys = setNKeys;
        this.nVals = setNVals;

        this.memIncr = setMemIncr;
        this.memIncr[0] = 0;
        this.outputsPerInput = setOutputsPerInput;

        this.outputLength = outputValIds.length;
    }

    @Override
    public void init(HadoopOpenCLContext clContext) {
        baseInit(clContext);
        this.setStrided(false);
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntUPairHadoopCLInputReducerBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntUPairHadoopCLOutputReducerBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer, HadoopCLOutputBuffer genericOutputBuffer) {
        IntUPairHadoopCLInputReducerBuffer inputBuffer = (IntUPairHadoopCLInputReducerBuffer)genericInputBuffer;
        IntUPairHadoopCLOutputReducerBuffer outputBuffer = (IntUPairHadoopCLOutputReducerBuffer)genericOutputBuffer;
        if(this.outputsPerInput < 0 && (outputBuffer.outputKeys == null || outputBuffer.outputKeys.length < inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey)) {
            outputBuffer.outputKeys = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
            outputBuffer.outputValIds = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
            outputBuffer.outputVals1 = new double[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
            outputBuffer.outputVals2 = new double[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
        }
        this.setup(inputBuffer.inputKeys, inputBuffer.inputValIds, inputBuffer.inputVals1, inputBuffer.inputVals2, outputBuffer.outputKeys, outputBuffer.outputValIds, outputBuffer.outputVals1, outputBuffer.outputVals2, inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals, outputBuffer.memIncr, this.outputsPerInput);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return true;
    }
    protected boolean write(int key, int valId, double val1, double val2) {
        this.javaProfile.stopKernel();
        this.javaProfile.startWrite();
        keyObj.set(key);
        valObj.set(valId, val1, val2);
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
        this.bufferedValId = new HadoopCLResizableIntArray();
        this.bufferedVal1 = new HadoopCLResizableDoubleArray();
        this.bufferedVal2 = new HadoopCLResizableDoubleArray();
        while(ctx.nextKeyValue()) {
            this.javaProfile.startRead();
            IntWritable key = (IntWritable)ctx.getCurrentKey();
            Iterable<UniquePairWritable> values = (Iterable<UniquePairWritable>)ctx.getValues();
            this.bufferedValId.reset();
            this.bufferedVal1.reset();
            this.bufferedVal2.reset();
            for(UniquePairWritable v : values) {
                this.bufferedValId.add(v.getIVal());
                this.bufferedVal1.add(v.getVal1());
                this.bufferedVal2.add(v.getVal2());
            }
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            reduce(key.get()
, new HadoopCLUPairValueIterator(
                       (int[])this.bufferedValId.getArray(),
                       (double[])this.bufferedVal1.getArray(),
                       (double[])this.bufferedVal2.getArray(),
                       this.bufferedValId.size()));
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

