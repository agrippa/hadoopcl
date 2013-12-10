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

public abstract class DoubleDoubleIntIntHadoopCLMapperKernel extends HadoopCLMapperKernel {
    protected int outputLength;


    public double[] inputKeys;
    public double[] inputVals;
    public int[] outputKeys;
    public int[] outputVals;
    final private IntWritable keyObj = new IntWritable();
    final private IntWritable valObj = new IntWritable();

    protected abstract void map(double key, double val);

    public void setup(double[] setInputkeys, double[] setInputvals, int[] setOutputkeys, int[] setOutputvals, int[] setNWrites, int setNPairs, int[] setMemIncr, int setOutputsPerInput) {
        this.inputKeys = setInputkeys;
        this.inputVals = setInputvals;
        this.outputKeys = setOutputkeys;
        this.outputVals = setOutputvals;
        this.nWrites = setNWrites;
        this.nPairs = setNPairs;

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

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return DoubleDoubleHadoopCLInputMapperBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntIntHadoopCLOutputMapperBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer, HadoopCLOutputBuffer genericOutputBuffer) {
        DoubleDoubleHadoopCLInputMapperBuffer inputBuffer = (DoubleDoubleHadoopCLInputMapperBuffer)genericInputBuffer;
        IntIntHadoopCLOutputMapperBuffer outputBuffer = (IntIntHadoopCLOutputMapperBuffer)genericOutputBuffer;
        this.setup(inputBuffer.inputKeys, inputBuffer.inputVals, outputBuffer.outputKeys, outputBuffer.outputVals, inputBuffer.nWrites, inputBuffer.nPairs, outputBuffer.memIncr, this.outputsPerInput);
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
    protected void callMap() {
        map(inputKeys[3], inputVals[3]);
    }
    @Override
    public HadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException {
        Context ctx = (Context)context;
        this.javaProfile = new HadoopCLAccumulatedProfile();
        this.javaProfile.startOverall();
        while(ctx.nextKeyValue()) {
            this.javaProfile.startRead();
            DoubleWritable key = (DoubleWritable)ctx.getCurrentKey();
            DoubleWritable val = (DoubleWritable)ctx.getCurrentValue();
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            map(key.get()
, val.get()
);
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

