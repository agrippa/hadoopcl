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

public abstract class DoubleDoubleIntPairHadoopCLMapperKernel extends HadoopCLMapperKernel {
    public int outputLength;


    public  double[] inputKeys;
    public  double[] inputVals;
    public  int[] outputKeys;
    public  double[] outputVals1;
    public  double[] outputVals2;

    protected abstract void map(double key, double val);

    public void postKernelSetup(int[] setOutputkeys, double[] setOutputvals1, double[] setOutputvals2, int[] setMemIncr, int[] setNWrites, int[] outputIterMarkers) {
        this.outputKeys = setOutputkeys;
        this.outputVals1 = setOutputvals1;
        this.outputVals2 = setOutputvals2;

        this.memIncr = setMemIncr;
        this.nWrites = setNWrites;

        this.outputIterMarkers = outputIterMarkers;
    }

    public void preKernelSetup(double[] setInputkeys, double[] setInputvals, int[] setNWrites, int setNPairs) {
        this.inputKeys = setInputkeys;
        this.inputVals = setInputvals;
        this.outputKeys = null;
        this.outputVals1 = null;
        this.outputVals2 = null;
        this.nWrites = setNWrites;
        this.nPairs = setNPairs;

        this.memIncr = null;

        this.outputLength = this.getArrayLength("outputVals1");
        this.outputIterMarkers = null;
    }

    public DoubleDoubleIntPairHadoopCLMapperKernel() {
        super(null, null);
        throw new UnsupportedOperationException();
    }
    public DoubleDoubleIntPairHadoopCLMapperKernel(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.setStrided(false);

        this.arrayLengths.put("outputIterMarkers", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("memIncr", 1);
        this.arrayLengths.put("outputKeys", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputVals1", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputVals2", this.clContext.getOutputBufferSize());
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return DoubleDoubleHadoopCLInputMapperBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntPairHadoopCLOutputMapperBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer) {
        DoubleDoubleHadoopCLInputMapperBuffer inputBuffer = (DoubleDoubleHadoopCLInputMapperBuffer)genericInputBuffer;
        this.preKernelSetup(inputBuffer.inputKeys, inputBuffer.inputVals, inputBuffer.nWrites, inputBuffer.nPairs);
    }

    @Override
    public void prepareForRead(HadoopCLOutputBuffer genericOutputBuffer) {
        IntPairHadoopCLOutputMapperBuffer outputBuffer = (IntPairHadoopCLOutputMapperBuffer)genericOutputBuffer;
        this.postKernelSetup(outputBuffer.outputKeys, outputBuffer.outputVals1, outputBuffer.outputVals2, outputBuffer.memIncr, outputBuffer.nWrites, outputBuffer.outputIterMarkers);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return false;
    }

    private final IntWritable keyObj = new IntWritable();
    private final PairWritable valObj = new PairWritable();

    protected boolean write(int key, double val1, double val2) {
        this.javaProfile.stopKernel();
        this.javaProfile.startWrite();
        keyObj.set(key);
        valObj.set(val1, val2);
        try { clContext.getContext().write(keyObj, valObj); } catch(Exception ex) { throw new RuntimeException(ex); }
        this.javaProfile.stopWrite();
        this.javaProfile.startKernel();
        return true;
    }

    @Override
    protected void callMap() {
        map(inputKeys[3], inputVals[3]);
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
            DoubleWritable key = (DoubleWritable)ctx.getCurrentKey();
            DoubleWritable val = (DoubleWritable)ctx.getCurrentValue();
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            map(key.get(), val.get()
);
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

