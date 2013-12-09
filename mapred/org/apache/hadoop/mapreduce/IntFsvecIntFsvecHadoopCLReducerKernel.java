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

public abstract class IntFsvecIntFsvecHadoopCLReducerKernel extends HadoopCLReducerKernel {
    protected int outputLength;

    protected int individualInputValsCount;
    protected int[] memAuxIntIncr;
    protected int[] memAuxFloatIncr;
    protected int outputAuxLength;

    public int[] inputKeys;
    public int[] inputValLookAsideBuffer;
    public int[] inputValIndices;
    public float[] inputValVals;
    public int[] outputKeys;
    public int[] outputValIntLookAsideBuffer;
    public int[] outputValFloatLookAsideBuffer;
    public int[] outputValIndices;
    public float[] outputValVals;
    private int[] bufferOutputIndices = null;
    private float[] bufferOutputVals = null;
    public int[] outputValLengthBuffer;
    final private IntWritable keyObj = new IntWritable();
    final private FSparseVectorWritable valObj = new FSparseVectorWritable();

    protected abstract void reduce(int key, HadoopCLFsvecValueIterator valIter);

    public void setup(int[] setInputkeys, int[] setInputvalLookAsideBuffer, int[] setInputvalIndices, float[] setInputvalVals, int[] setOutputkeys, int[] setOutputvalIntLookAsideBuffer, int[] setOutputvalFloatLookAsideBuffer, int[] setOutputvalIndices, float[] setOutputvalVals, int[] setOutputValLengthBuffer, int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals, int setIndividualInputValsCount, int[] setMemAuxIntIncr, int[] setMemAuxFloatIncr, int[] setMemIncr, int setOutputsPerInput) {
        this.inputKeys = setInputkeys;
        this.inputValLookAsideBuffer = setInputvalLookAsideBuffer;
        this.inputValIndices = setInputvalIndices;
        this.inputValVals = setInputvalVals;
        this.outputKeys = setOutputkeys;
        this.outputValIntLookAsideBuffer = setOutputvalIntLookAsideBuffer;
        this.outputValFloatLookAsideBuffer = setOutputvalFloatLookAsideBuffer;
        this.outputValIndices = setOutputvalIndices;
        this.outputValVals = setOutputvalVals;
        this.outputValLengthBuffer = setOutputValLengthBuffer;
        this.input_keyIndex = setKeyIndex;
        this.nWrites = setNWrites;
        this.nKeys = setNKeys;
        this.nVals = setNVals;

        this.memIncr = setMemIncr;
        this.memIncr[0] = 0;
        this.outputsPerInput = setOutputsPerInput;

        this.outputLength = outputValIntLookAsideBuffer.length;
        this.outputAuxLength = outputValIndices.length;
        this.individualInputValsCount = setIndividualInputValsCount;

        this.memAuxIntIncr = setMemAuxIntIncr;
        this.memAuxFloatIncr = setMemAuxFloatIncr;
        this.memAuxIntIncr[0] = 0;
        this.memAuxFloatIncr[0] = 0;

    }

    @Override
    public void init(HadoopOpenCLContext clContext) {
        baseInit(clContext);
        this.setStrided(false);
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntFsvecHadoopCLInputReducerBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntFsvecHadoopCLOutputReducerBuffer.class; }
    protected int[] allocInt(int len) {
        return new int[len];
    }
    protected double[] allocDouble(int len) {
        return new double[len];
    }
    protected float[] allocFloat(int len) {
        return new float[len];
    }


    protected int inputVectorLength(int vid) {
       return 0;
    }
    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer, HadoopCLOutputBuffer genericOutputBuffer) {
        IntFsvecHadoopCLInputReducerBuffer inputBuffer = (IntFsvecHadoopCLInputReducerBuffer)genericInputBuffer;
        IntFsvecHadoopCLOutputReducerBuffer outputBuffer = (IntFsvecHadoopCLOutputReducerBuffer)genericOutputBuffer;
        if(this.outputsPerInput < 0 && (outputBuffer.outputKeys == null || outputBuffer.outputKeys.length < inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey)) {
            outputBuffer.outputKeys = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
            outputBuffer.outputValIntLookAsideBuffer = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];

            outputBuffer.outputValFloatLookAsideBuffer = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];

            int bigger = this.clContext.getPreallocLength() > (inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey) * 5 ? this.clContext.getPreallocLength() : (inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey) * 5;

            outputBuffer.outputValIndices = new int[bigger];

            outputBuffer.outputValVals = new float[bigger];

        }
        this.setup(inputBuffer.inputKeys, inputBuffer.inputValLookAsideBuffer, inputBuffer.inputValIndices, inputBuffer.inputValVals, outputBuffer.outputKeys, outputBuffer.outputValIntLookAsideBuffer, outputBuffer.outputValFloatLookAsideBuffer, outputBuffer.outputValIndices, outputBuffer.outputValVals, outputBuffer.outputValLengthBuffer, inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals, inputBuffer.individualInputValsCount, outputBuffer.memAuxIntIncr, outputBuffer.memAuxFloatIncr, outputBuffer.memIncr, this.outputsPerInput);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return true;
    }
    protected boolean write(int key, int[] valIndices, float[] valVals, int len) {
        this.javaProfile.stopKernel();
        this.javaProfile.startWrite();
        keyObj.set(key);
        valObj.set(valIndices, valVals, len);
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
            Iterable<FSparseVectorWritable> values = (Iterable<FSparseVectorWritable>)ctx.getValues();
            List<int[]> accIndices = new ArrayList<int[]>();
            List<float[]> accVals = new ArrayList<float[]>();
            for(FSparseVectorWritable v : values) {
                accIndices.add(v.indices());
                accVals.add(v.vals());
            }
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            reduce(key.get()
, new HadoopCLFsvecValueIterator(
                       accIndices, accVals));
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

