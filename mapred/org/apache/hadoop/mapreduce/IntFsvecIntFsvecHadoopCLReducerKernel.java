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

public abstract class IntFsvecIntFsvecHadoopCLReducerKernel extends HadoopCLReducerKernel {
    public int outputLength;

    public int individualInputValsCount;
    public int[] memAuxIntIncr;
    public int[] memAuxFloatIncr;
    public int outputAuxIntLength;
    public int outputAuxFloatLength;

    public  int[] inputKeys;
    public  int[] inputValLookAsideBuffer;
    public  int[] inputValIndices;
    public  float[] inputValVals;
    public  int[] outputKeys;
    public  int[] outputValIntLookAsideBuffer;
    public  int[] outputValFloatLookAsideBuffer;
    public  int[] outputValIndices;
    public  float[] outputValVals;
    public  int[] outputValLengthBuffer;

    protected abstract void reduce(int key, HadoopCLFsvecValueIterator valIter);

    public void postKernelSetup(int[] setOutputkeys, int[] setOutputvalIntLookAsideBuffer, int[] setOutputvalFloatLookAsideBuffer, int[] setOutputvalIndices, float[] setOutputvalVals, int[] setOutputValLengthBuffer, int[] setMemAuxIntIncr, int[] setMemAuxFloatIncr, int[] setMemIncr, int[] setNWrites, int[] outputIterMarkers) {
        this.outputKeys = setOutputkeys;
        this.outputValIntLookAsideBuffer = setOutputvalIntLookAsideBuffer;
        this.outputValFloatLookAsideBuffer = setOutputvalFloatLookAsideBuffer;
        this.outputValIndices = setOutputvalIndices;
        this.outputValVals = setOutputvalVals;
        this.outputValLengthBuffer = setOutputValLengthBuffer;

        this.memIncr = setMemIncr;
        this.nWrites = setNWrites;

        this.outputIterMarkers = outputIterMarkers;
        this.memAuxIntIncr = setMemAuxIntIncr;
        this.memAuxFloatIncr = setMemAuxFloatIncr;
        this.memAuxIntIncr[0] = 0;
        this.memAuxFloatIncr[0] = 0;

    }

    public void preKernelSetup(int[] setInputkeys, int[] setInputvalLookAsideBuffer, int[] setInputvalIndices, float[] setInputvalVals, int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals, int setIndividualInputValsCount) {
        this.inputKeys = setInputkeys;
        this.inputValLookAsideBuffer = setInputvalLookAsideBuffer;
        this.inputValIndices = setInputvalIndices;
        this.inputValVals = setInputvalVals;
        this.outputKeys = null;
        this.outputValIntLookAsideBuffer = null;
        this.outputValFloatLookAsideBuffer = null;
        this.outputValIndices = null;
        this.outputValVals = null;
        this.outputValLengthBuffer = null;
        this.input_keyIndex = setKeyIndex;
        this.nWrites = setNWrites;
        this.nKeys = setNKeys;
        this.nVals = setNVals;

        this.memIncr = null;

        this.outputLength = this.getArrayLength("outputValIntLookAsideBuffer");
        this.outputAuxIntLength = this.getArrayLength("outputValIndices");
        this.outputAuxFloatLength = this.getArrayLength("outputValVals");
        this.outputIterMarkers = null;
        this.individualInputValsCount = setIndividualInputValsCount;

        this.memAuxIntIncr = null;
        this.memAuxFloatIncr = null;

    }

    public IntFsvecIntFsvecHadoopCLReducerKernel() {
        super(null, null);
        throw new UnsupportedOperationException();
    }
    public IntFsvecIntFsvecHadoopCLReducerKernel(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.setStrided(false);

        this.arrayLengths.put("outputIterMarkers", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("memIncr", 1);
        this.arrayLengths.put("outputKeys", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputValIntLookAsideBuffer", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputValFloatLookAsideBuffer", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputValIndices", this.clContext.getPreallocIntLength());
        this.arrayLengths.put("outputValVals", this.clContext.getPreallocFloatLength());
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
    public void fill(HadoopCLInputBuffer genericInputBuffer) {
        IntFsvecHadoopCLInputReducerBuffer inputBuffer = (IntFsvecHadoopCLInputReducerBuffer)genericInputBuffer;
        this.preKernelSetup(inputBuffer.inputKeys, inputBuffer.inputValLookAsideBuffer, inputBuffer.inputValIndices, inputBuffer.inputValVals, inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals, inputBuffer.individualInputValsCount);
    }

    @Override
    public void prepareForRead(HadoopCLOutputBuffer genericOutputBuffer) {
        IntFsvecHadoopCLOutputReducerBuffer outputBuffer = (IntFsvecHadoopCLOutputReducerBuffer)genericOutputBuffer;
        this.postKernelSetup(outputBuffer.outputKeys, outputBuffer.outputValIntLookAsideBuffer, outputBuffer.outputValFloatLookAsideBuffer, outputBuffer.outputValIndices, outputBuffer.outputValVals, outputBuffer.outputValLengthBuffer, outputBuffer.memAuxIntIncr, outputBuffer.memAuxFloatIncr, outputBuffer.memIncr, outputBuffer.nWrites, outputBuffer.outputIterMarkers);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return true;
    }

    private final IntWritable keyObj = new IntWritable();
    private final FSparseVectorWritable valObj = new FSparseVectorWritable();

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


    protected boolean write(int key, int[] valIndices, int indicesOffset, float[] valVals, int valsOffset, int len) {
        this.javaProfile.stopKernel();
        this.javaProfile.startWrite();
        keyObj.set(key);
        valObj.set(valIndices, indicesOffset, valVals, valsOffset, len);
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
            reduce(key.get(),             new HadoopCLFsvecValueIterator(
                       accIndices, accVals));
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

