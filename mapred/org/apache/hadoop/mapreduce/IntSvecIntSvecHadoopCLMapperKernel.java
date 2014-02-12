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
import org.apache.hadoop.mapreduce.Mapper.Context;

public abstract class IntSvecIntSvecHadoopCLMapperKernel extends HadoopCLMapperKernel {
    public int outputLength;

    public int individualInputValsCount;
    public int currentInputVectorLength = -1;
    public int[] memAuxIntIncr;
    public int[] memAuxDoubleIncr;
    public int outputAuxIntLength;
    public int outputAuxDoubleLength;

    public  int[] inputKeys;
    public  int[] inputValLookAsideBuffer;
    public  int[] inputValIndices;
    public  double[] inputValVals;
    public  int[] outputKeys;
    public  int[] outputValIntLookAsideBuffer;
    public  int[] outputValDoubleLookAsideBuffer;
    public  int[] outputValIndices;
    public  double[] outputValVals;
    public  int[] outputValLengthBuffer;

    protected abstract void map(int key, int[] valIndices, double[] valVals, int len);

    public void postKernelSetup(int[] setOutputkeys, int[] setOutputvalIntLookAsideBuffer, int[] setOutputvalDoubleLookAsideBuffer, int[] setOutputvalIndices, double[] setOutputvalVals, int[] setOutputValLengthBuffer, int[] setMemAuxIntIncr, int[] setMemAuxDoubleIncr, int[] setMemIncr, int[] setNWrites, int[] outputIterMarkers) {
        this.outputKeys = setOutputkeys;
        this.outputValIntLookAsideBuffer = setOutputvalIntLookAsideBuffer;
        this.outputValDoubleLookAsideBuffer = setOutputvalDoubleLookAsideBuffer;
        this.outputValIndices = setOutputvalIndices;
        this.outputValVals = setOutputvalVals;
        this.outputValLengthBuffer = setOutputValLengthBuffer;

        this.memIncr = setMemIncr;
        this.nWrites = setNWrites;

        this.outputIterMarkers = outputIterMarkers;
        this.memAuxIntIncr = setMemAuxIntIncr;
        this.memAuxDoubleIncr = setMemAuxDoubleIncr;
        this.memAuxIntIncr[0] = 0;
        this.memAuxDoubleIncr[0] = 0;

    }

    public void preKernelSetup(int[] setInputkeys, int[] setInputvalLookAsideBuffer, int[] setInputvalIndices, double[] setInputvalVals, int[] setNWrites, int setNPairs, int setIndividualInputValsCount) {
        this.inputKeys = setInputkeys;
        this.inputValLookAsideBuffer = setInputvalLookAsideBuffer;
        this.inputValIndices = setInputvalIndices;
        this.inputValVals = setInputvalVals;
        this.outputKeys = null;
        this.outputValIntLookAsideBuffer = null;
        this.outputValDoubleLookAsideBuffer = null;
        this.outputValIndices = null;
        this.outputValVals = null;
        this.outputValLengthBuffer = null;
        this.nWrites = setNWrites;
        this.nPairs = setNPairs;

        this.memIncr = null;

        this.outputLength = this.getArrayLength("outputValIntLookAsideBuffer");
        this.outputAuxIntLength = this.getArrayLength("outputValIndices");
        this.outputAuxDoubleLength = this.getArrayLength("outputValVals");
        this.outputIterMarkers = null;
        this.individualInputValsCount = setIndividualInputValsCount;

        this.memAuxIntIncr = null;
        this.memAuxDoubleIncr = null;

    }

    public IntSvecIntSvecHadoopCLMapperKernel() {
        super(null, null);
        throw new UnsupportedOperationException();
    }
    public IntSvecIntSvecHadoopCLMapperKernel(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.setStrided(false);

        this.arrayLengths.put("outputIterMarkers", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("memIncr", 1);
        this.arrayLengths.put("outputKeys", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputValIntLookAsideBuffer", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputValDoubleLookAsideBuffer", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputValIndices", this.clContext.getPreallocIntLength());
        this.arrayLengths.put("outputValVals", this.clContext.getPreallocDoubleLength());
        this.arrayLengths.put("outputValLengthBuffer", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("memAuxIntIncr", 1);
        this.arrayLengths.put("memAuxDoubleIncr", 1);
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntSvecHadoopCLInputMapperBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntSvecHadoopCLOutputMapperBuffer.class; }
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
       return this.currentInputVectorLength;
    }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer) {
        IntSvecHadoopCLInputMapperBuffer inputBuffer = (IntSvecHadoopCLInputMapperBuffer)genericInputBuffer;
        this.setStrided(inputBuffer.enableStriding);

        if (inputBuffer.enableStriding) {
            int index = 0;
            inputBuffer.individualInputValsCount = 0;
            Iterator<Integer> lengthIter = inputBuffer.sortedVals.descendingKeySet().iterator();
            while (lengthIter.hasNext()) {
                LinkedList<IndValWrapper> pairs = inputBuffer.sortedVals.get(lengthIter.next());
                Iterator<IndValWrapper> pairsIter = pairs.iterator();
                while (pairsIter.hasNext()) {
                    IndValWrapper curr = pairsIter.next();
                    inputBuffer.inputValLookAsideBuffer[index] = inputBuffer.individualInputValsCount;
                    inputBuffer.inputValIndices = ensureCapacity(inputBuffer.inputValIndices, (index + ((curr.length - 1) * inputBuffer.nPairs)) + 1);
                    inputBuffer.inputValVals = ensureCapacity(inputBuffer.inputValVals, (index + ((curr.length - 1) * inputBuffer.nPairs)) + 1);
                    for (int i = 0; i < curr.length; i++) {
                        inputBuffer.inputValIndices[index + (i * inputBuffer.nPairs)] = curr.indices[i];
                        inputBuffer.inputValVals[index + (i * inputBuffer.nPairs)] = curr.dvals[i];
                    }
                    inputBuffer.individualInputValsCount += curr.length;
                    index++;
                } // while (pairsIter)
            } // while (lengthIter)
        } // if (enableStriding)

        this.preKernelSetup(inputBuffer.inputKeys, inputBuffer.inputValLookAsideBuffer, inputBuffer.inputValIndices, inputBuffer.inputValVals, inputBuffer.nWrites, inputBuffer.nPairs, inputBuffer.individualInputValsCount);
    }

    @Override
    public void prepareForRead(HadoopCLOutputBuffer genericOutputBuffer) {
        IntSvecHadoopCLOutputMapperBuffer outputBuffer = (IntSvecHadoopCLOutputMapperBuffer)genericOutputBuffer;
        this.postKernelSetup(outputBuffer.outputKeys, outputBuffer.outputValIntLookAsideBuffer, outputBuffer.outputValDoubleLookAsideBuffer, outputBuffer.outputValIndices, outputBuffer.outputValVals, outputBuffer.outputValLengthBuffer, outputBuffer.memAuxIntIncr, outputBuffer.memAuxDoubleIncr, outputBuffer.memIncr, outputBuffer.nWrites, outputBuffer.outputIterMarkers);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return true;
    }

    private final IntWritable keyObj = new IntWritable();
    private final SparseVectorWritable valObj = new SparseVectorWritable();

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


    protected boolean write(int key, int[] valIndices, int indicesOffset, double[] valVals, int valsOffset, int len) {
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
    protected void callMap() {
        map(inputKeys[3], inputValIndices, inputValVals, inputValLookAsideBuffer[3] + this.nPairs + this.individualInputValsCount);
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
            SparseVectorWritable val = (SparseVectorWritable)ctx.getCurrentValue();
            this.currentInputVectorLength = val.size();
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            map(key.get(), val.indices(), val.vals(), val.size()
);
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

