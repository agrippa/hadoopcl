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

public abstract class PairPairDoubleDoubleHadoopCLReducerKernel extends HadoopCLReducerKernel {
    protected int outputLength;
    protected HadoopCLResizableDoubleArray bufferedVal1 = null;
    protected HadoopCLResizableDoubleArray bufferedVal2 = null;


    public double[] inputKeys1;
    public double[] inputKeys2;
    public double[] inputVals1;
    public double[] inputVals2;
    public double[] outputKeys;
    public double[] outputVals;
    final private DoubleWritable keyObj = new DoubleWritable();
    final private DoubleWritable valObj = new DoubleWritable();

    protected abstract void reduce(double key1, double key2, HadoopCLPairValueIterator valIter);

    public void setup(double[] setInputkeys1, double[] setInputkeys2, double[] setInputvals1, double[] setInputvals2, double[] setOutputkeys, double[] setOutputvals, int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals, int[] setMemIncr, int setOutputsPerInput) {
        this.inputKeys1 = setInputkeys1;
        this.inputKeys2 = setInputkeys2;
        this.inputVals1 = setInputvals1;
        this.inputVals2 = setInputvals2;
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

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return PairPairHadoopCLInputReducerBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return DoubleDoubleHadoopCLOutputReducerBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer, HadoopCLOutputBuffer genericOutputBuffer) {
        PairPairHadoopCLInputReducerBuffer inputBuffer = (PairPairHadoopCLInputReducerBuffer)genericInputBuffer;
        DoubleDoubleHadoopCLOutputReducerBuffer outputBuffer = (DoubleDoubleHadoopCLOutputReducerBuffer)genericOutputBuffer;
        if(this.outputsPerInput < 0 && (outputBuffer.outputKeys == null || outputBuffer.outputKeys.length < inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey)) {
            outputBuffer.outputKeys = new double[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
            outputBuffer.outputVals = new double[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
        }
        this.setup(inputBuffer.inputKeys1, inputBuffer.inputKeys2, inputBuffer.inputVals1, inputBuffer.inputVals2, outputBuffer.outputKeys, outputBuffer.outputVals, inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals, outputBuffer.memIncr, this.outputsPerInput);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return false;
    }
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
        reduce(inputKeys1[3], inputKeys2[3], null);
    }
    @Override
    public HadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException {
        Context ctx = (Context)context;
        this.javaProfile = new HadoopCLAccumulatedProfile();
        this.javaProfile.startOverall();
        this.bufferedVal1 = new HadoopCLResizableDoubleArray();
        this.bufferedVal2 = new HadoopCLResizableDoubleArray();
        while(ctx.nextKeyValue()) {
            this.javaProfile.startRead();
            PairWritable key = (PairWritable)ctx.getCurrentKey();
            Iterable<PairWritable> values = (Iterable<PairWritable>)ctx.getValues();
            this.bufferedVal1.reset();
            this.bufferedVal2.reset();
            for(PairWritable v : values) {
                this.bufferedVal1.add(v.getVal1());
                this.bufferedVal2.add(v.getVal2());
            }
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            reduce(key.getVal1(), key.getVal2()
, new HadoopCLPairValueIterator(
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

