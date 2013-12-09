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

public abstract class IntSvecIntSvecHadoopCLReducerKernel extends HadoopCLReducerKernel {
    protected int outputLength;

    protected int individualInputValsCount;
    protected int[] memAuxIntIncr;
    protected int[] memAuxDoubleIncr;
    protected int outputAuxLength;

    public int[] inputKeys;
    public int[] inputValLookAsideBuffer;
    public int[] inputValIndices;
    public double[] inputValVals;
    public int[] outputKeys;
    public int[] outputValIntLookAsideBuffer;
    public int[] outputValDoubleLookAsideBuffer;
    public int[] outputValIndices;
    public double[] outputValVals;
    private int[] bufferOutputIndices = null;
    private double[] bufferOutputVals = null;
    public int[] outputValLengthBuffer;
    final private IntWritable keyObj = new IntWritable();
    final private SparseVectorWritable valObj = new SparseVectorWritable();

    protected abstract void reduce(int key, HadoopCLSvecValueIterator valIter);

    public void setup(int[] setInputkeys, int[] setInputvalLookAsideBuffer, int[] setInputvalIndices, double[] setInputvalVals, int[] setOutputkeys, int[] setOutputvalIntLookAsideBuffer, int[] setOutputvalDoubleLookAsideBuffer, int[] setOutputvalIndices, double[] setOutputvalVals, int[] setOutputValLengthBuffer, int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals, int setIndividualInputValsCount, int[] setMemAuxIntIncr, int[] setMemAuxDoubleIncr, int[] setMemIncr, int setOutputsPerInput) {
        this.inputKeys = setInputkeys;
        this.inputValLookAsideBuffer = setInputvalLookAsideBuffer;
        this.inputValIndices = setInputvalIndices;
        this.inputValVals = setInputvalVals;
        this.outputKeys = setOutputkeys;
        this.outputValIntLookAsideBuffer = setOutputvalIntLookAsideBuffer;
        this.outputValDoubleLookAsideBuffer = setOutputvalDoubleLookAsideBuffer;
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
        this.memAuxDoubleIncr = setMemAuxDoubleIncr;
        this.memAuxIntIncr[0] = 0;
        this.memAuxDoubleIncr[0] = 0;

    }

    @Override
    public void init(HadoopOpenCLContext clContext) {
        baseInit(clContext);
        this.setStrided(false);
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntSvecHadoopCLInputReducerBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntSvecHadoopCLOutputReducerBuffer.class; }
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
        IntSvecHadoopCLInputReducerBuffer inputBuffer = (IntSvecHadoopCLInputReducerBuffer)genericInputBuffer;
        IntSvecHadoopCLOutputReducerBuffer outputBuffer = (IntSvecHadoopCLOutputReducerBuffer)genericOutputBuffer;
        if(this.outputsPerInput < 0 && (outputBuffer.outputKeys == null || outputBuffer.outputKeys.length < inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey)) {
            outputBuffer.outputKeys = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];
            outputBuffer.outputValIntLookAsideBuffer = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];

            outputBuffer.outputValDoubleLookAsideBuffer = new int[inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey];

            int bigger = this.clContext.getPreallocLength() > (inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey) * 5 ? this.clContext.getPreallocLength() : (inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey) * 5;

            outputBuffer.outputValIndices = new int[bigger];

            outputBuffer.outputValVals = new double[bigger];

            outputValLengthBuffer = new int[this.clContext.getBufferSize() * outputsPerInput];
            memAuxIntIncr = new int[1];
            memAuxDoubleIncr = new int[1];
        }
        this.setup(inputBuffer.inputKeys, inputBuffer.inputValLookAsideBuffer, inputBuffer.inputValIndices, inputBuffer.inputValVals, outputBuffer.outputKeys, outputBuffer.outputValIntLookAsideBuffer, outputBuffer.outputValDoubleLookAsideBuffer, outputBuffer.outputValIndices, outputBuffer.outputValVals, outputBuffer.outputValLengthBuffer, inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals, inputBuffer.individualInputValsCount, outputBuffer.memAuxIntIncr, outputBuffer.memAuxDoubleIncr, outputBuffer.memIncr, this.outputsPerInput);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return true;
    }
    protected boolean write(int key, int[] valIndices, double[] valVals, int len) {
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
            Iterable<SparseVectorWritable> values = (Iterable<SparseVectorWritable>)ctx.getValues();
            List<int[]> accIndices = new ArrayList<int[]>();
            List<double[]> accVals = new ArrayList<double[]>();
            for(SparseVectorWritable v : values) {
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

