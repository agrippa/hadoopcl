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

public abstract class IntIntIntIntHadoopCLMapperKernel extends HadoopCLMapperKernel {
    public int outputLength;


    public  int[] inputKeys;
    public  int[] inputVals;
    public  int[] outputKeys;
    public  int[] outputVals;

    protected abstract void map(int key, int val);

    public void postKernelSetup(int[] setOutputkeys, int[] setOutputvals, int[] setMemIncr, int[] setNWrites, int[] outputIterMarkers) {
        this.outputKeys = setOutputkeys;
        this.outputVals = setOutputvals;

        this.memIncr = setMemIncr;
        this.nWrites = setNWrites;

        this.outputIterMarkers = outputIterMarkers;
    }

    public void preKernelSetup(int[] setInputkeys, int[] setInputvals, int[] setNWrites, int setNPairs) {
        this.inputKeys = setInputkeys;
        this.inputVals = setInputvals;
        this.outputKeys = null;
        this.outputVals = null;
        this.nWrites = setNWrites;
        this.nPairs = setNPairs;

        this.memIncr = null;

        this.outputLength = this.getArrayLength("outputVals");
        this.outputIterMarkers = null;
    }

    public IntIntIntIntHadoopCLMapperKernel() {
        super(null, null);
        throw new UnsupportedOperationException();
    }
    public IntIntIntIntHadoopCLMapperKernel(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.setStrided(false);

        this.arrayLengths.put("outputIterMarkers", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("memIncr", 1);
        this.arrayLengths.put("outputKeys", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputVals", this.clContext.getOutputBufferSize());
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntIntHadoopCLInputMapperBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntIntHadoopCLOutputMapperBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer) {
        IntIntHadoopCLInputMapperBuffer inputBuffer = (IntIntHadoopCLInputMapperBuffer)genericInputBuffer;
        this.preKernelSetup(inputBuffer.inputKeys, inputBuffer.inputVals, inputBuffer.nWrites, inputBuffer.nPairs);
    }

    @Override
    public void prepareForRead(HadoopCLOutputBuffer genericOutputBuffer) {
        IntIntHadoopCLOutputMapperBuffer outputBuffer = (IntIntHadoopCLOutputMapperBuffer)genericOutputBuffer;
        this.postKernelSetup(outputBuffer.outputKeys, outputBuffer.outputVals, outputBuffer.memIncr, outputBuffer.nWrites, outputBuffer.outputIterMarkers);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return true;
    }

    private final IntWritable keyObj = new IntWritable();
    private final IntWritable valObj = new IntWritable();

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
            IntWritable key = (IntWritable)ctx.getCurrentKey();
            IntWritable val = (IntWritable)ctx.getCurrentValue();
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

