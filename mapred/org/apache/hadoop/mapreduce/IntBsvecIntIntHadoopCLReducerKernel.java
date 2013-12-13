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

public abstract class IntBsvecIntIntHadoopCLReducerKernel extends HadoopCLReducerKernel {
    protected int outputLength;

    protected int individualInputValsCount;

    public int[] inputKeys;
    public int[] inputValLookAsideBuffer;
    public int[] inputValIndices;
    public double[] inputValVals;
    public int[] outputKeys;
    public int[] outputVals;
    final private IntWritable keyObj = new IntWritable();
    final private IntWritable valObj = new IntWritable();

    protected abstract void reduce(int key, HadoopCLSvecValueIterator valIter);

    public void setup(int[] setInputkeys, int[] setInputvalLookAsideBuffer, int[] setInputvalIndices, double[] setInputvalVals, int[] setOutputkeys, int[] setOutputvals, int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals, int setIndividualInputValsCount, int[] setMemIncr, int setOutputsPerInput) {
        this.inputKeys = setInputkeys;
        this.inputValLookAsideBuffer = setInputvalLookAsideBuffer;
        this.inputValIndices = setInputvalIndices;
        this.inputValVals = setInputvalVals;
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
        this.individualInputValsCount = setIndividualInputValsCount;

    }

    @Override
    public void init(HadoopOpenCLContext clContext) {
        baseInit(clContext);
        this.setStrided(false);
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntBsvecHadoopCLInputReducerBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntIntHadoopCLOutputReducerBuffer.class; }

    protected int inputVectorLength(int vid) {
       return 0;
    }
    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer, HadoopCLOutputBuffer genericOutputBuffer) {
        IntBsvecHadoopCLInputReducerBuffer inputBuffer = (IntBsvecHadoopCLInputReducerBuffer)genericInputBuffer;
        IntIntHadoopCLOutputReducerBuffer outputBuffer = (IntIntHadoopCLOutputReducerBuffer)genericOutputBuffer;
        if(this.outputsPerInput < 0 && (outputBuffer.outputKeys == null || outputBuffer.outputKeys.length < inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey)) {
            outputBuffer.outputKeys = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
            outputBuffer.outputVals = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
        }
        this.setup(inputBuffer.inputKeys, inputBuffer.inputValLookAsideBuffer, inputBuffer.inputValIndices, inputBuffer.inputValVals, outputBuffer.outputKeys, outputBuffer.outputVals, inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals, inputBuffer.individualInputValsCount, outputBuffer.memIncr, this.outputsPerInput);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return false;
    }
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
    public HadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException {
        Context ctx = (Context)context;
        this.javaProfile = new HadoopCLAccumulatedProfile();
        this.javaProfile.startOverall();
        while(ctx.nextKeyValue()) {
            this.javaProfile.startRead();
            IntWritable key = (IntWritable)ctx.getCurrentKey();
            Iterable<BSparseVectorWritable> values = (Iterable<BSparseVectorWritable>)ctx.getValues();
            List<int[]> accIndices = new ArrayList<int[]>();
            List<double[]> accVals = new ArrayList<double[]>();
            for(BSparseVectorWritable v : values) {
                accIndices.add(v.indices());
                accVals.add(v.vals());
            }
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            reduce(key.get()
, new HadoopCLSvecValueIterator(
                       accIndices, accVals));
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

