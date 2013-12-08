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
import org.apache.hadoop.mapreduce.Mapper.Context;

public abstract class IntFsvecIntFsvecHadoopCLMapperKernel extends HadoopCLMapperKernel {
    protected int outputLength;

    protected int individualInputValsCount;
    protected int currentInputVectorLength = -1;
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

    protected abstract void map(int key, int[] valIndices, float[] valVals, int len);

    public void setup(int[] setInputkeys, int[] setInputvalLookAsideBuffer, int[] setInputvalIndices, float[] setInputvalVals, int[] setOutputkeys, int[] setOutputvalIntLookAsideBuffer, int[] setOutputvalFloatLookAsideBuffer, int[] setOutputvalIndices, float[] setOutputvalVals, int[] setOutputValLengthBuffer, int[] setNWrites, int setNPairs, int setIndividualInputValsCount, int[] setMemAuxIntIncr, int[] setMemAuxFloatIncr, int[] setMemIncr, int setOutputsPerInput) {
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
        this.nWrites = setNWrites;
        this.nPairs = setNPairs;

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

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntFsvecHadoopCLInputMapperBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntFsvecHadoopCLOutputMapperBuffer.class; }
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
    public void fill(HadoopCLInputBuffer genericInputBuffer, HadoopCLOutputBuffer genericOutputBuffer) {
        IntFsvecHadoopCLInputMapperBuffer inputBuffer = (IntFsvecHadoopCLInputMapperBuffer)genericInputBuffer;
        IntFsvecHadoopCLOutputMapperBuffer outputBuffer = (IntFsvecHadoopCLOutputMapperBuffer)genericOutputBuffer;
        this.setStrided(inputBuffer.enableStriding);

        if (inputBuffer.enableStriding) {
            int index = 0;
            Iterator<Integer> lengthIter = inputBuffer.sortedVals.descendingKeySet().iterator();
            while (lengthIter.hasNext()) {
                LinkedList<IndValWrapper> pairs = inputBuffer.sortedVals.get(lengthIter.next());
                Iterator<IndValWrapper> pairsIter = pairs.iterator();
                while (pairsIter.hasNext()) {
                    IndValWrapper curr = pairsIter.next();
                    inputBuffer.inputValLookAsideBuffer[index] = inputBuffer.individualInputValsCount;
                    inputBuffer.inputValIndices.ensureCapacity( (index + ((curr.length - 1) * inputBuffer.nVectorsToBuffer)) + 1);
                    inputBuffer.inputValVals.ensureCapacity( (index + ((curr.length - 1) * inputBuffer.nVectorsToBuffer)) + 1);
                    for (int i = 0; i < curr.length; i++) {
                        inputBuffer.inputValIndices.unsafeSet(index + (i * inputBuffer.nVectorsToBuffer), curr.indices[i]);
                        inputBuffer.inputValVals.unsafeSet(index + (i * inputBuffer.nVectorsToBuffer), curr.fvals[i]);
                    }
                    inputBuffer.individualInputValsCount += curr.length;
                    index++;
                } // while (pairsIter)
            } // while (lengthIter)
        } // if (enableStriding)

        this.setup(inputBuffer.inputKeys, inputBuffer.inputValLookAsideBuffer, (int[])(inputBuffer.inputValIndices.getArray()), (float[])(inputBuffer.inputValVals.getArray()), outputBuffer.outputKeys, outputBuffer.outputValIntLookAsideBuffer, outputBuffer.outputValFloatLookAsideBuffer, outputBuffer.outputValIndices, outputBuffer.outputValVals, outputBuffer.outputValLengthBuffer, inputBuffer.nWrites, inputBuffer.nPairs, inputBuffer.individualInputValsCount, outputBuffer.memAuxIntIncr, outputBuffer.memAuxFloatIncr, outputBuffer.memIncr, this.outputsPerInput);
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
    protected void callMap() {
        map(inputKeys[3], inputValIndices, inputValVals, inputValLookAsideBuffer[3] + this.nPairs + this.individualInputValsCount);
    }
    @Override
    public HadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException {
        Context ctx = (Context)context;
        this.javaProfile = new HadoopCLAccumulatedProfile();
        this.javaProfile.startOverall();
        while(ctx.nextKeyValue()) {
            this.javaProfile.startRead();
            IntWritable key = (IntWritable)ctx.getCurrentKey();
            FSparseVectorWritable val = (FSparseVectorWritable)ctx.getCurrentValue();
            this.currentInputVectorLength = val.size();
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            map(key.get()
, val.indices(), val.vals(), val.size()
);
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

