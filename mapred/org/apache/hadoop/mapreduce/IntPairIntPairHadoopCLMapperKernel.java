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

public abstract class IntPairIntPairHadoopCLMapperKernel extends HadoopCLMapperKernel {
    protected int outputLength;


    public int[] inputKeys;
    public double[] inputVals1;
    public double[] inputVals2;
    public int[] outputKeys;
    public double[] outputVals1;
    public double[] outputVals2;
    final private IntWritable keyObj = new IntWritable();
    final private PairWritable valObj = new PairWritable();

    protected abstract void map(int key, double val1, double val2);

    public void setup(int[] setInputkeys, double[] setInputvals1, double[] setInputvals2, int[] setOutputkeys, double[] setOutputvals1, double[] setOutputvals2, int[] setNWrites, int setNPairs, int[] setMemIncr, int setOutputsPerInput) {
        this.inputKeys = setInputkeys;
        this.inputVals1 = setInputvals1;
        this.inputVals2 = setInputvals2;
        this.outputKeys = setOutputkeys;
        this.outputVals1 = setOutputvals1;
        this.outputVals2 = setOutputvals2;
        this.nWrites = setNWrites;
        this.nPairs = setNPairs;

        this.memIncr = setMemIncr;
        this.memIncr[0] = 0;
        this.outputsPerInput = setOutputsPerInput;

        this.outputLength = outputVals1.length;
    }

    @Override
    public void init(HadoopOpenCLContext clContext) {
        baseInit(clContext);
        this.setStrided(false);
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return IntPairHadoopCLInputMapperBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntPairHadoopCLOutputMapperBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer, HadoopCLOutputBuffer genericOutputBuffer) {
        IntPairHadoopCLInputMapperBuffer inputBuffer = (IntPairHadoopCLInputMapperBuffer)genericInputBuffer;
        IntPairHadoopCLOutputMapperBuffer outputBuffer = (IntPairHadoopCLOutputMapperBuffer)genericOutputBuffer;
        this.setup(inputBuffer.inputKeys, inputBuffer.inputVals1, inputBuffer.inputVals2, outputBuffer.outputKeys, outputBuffer.outputVals1, outputBuffer.outputVals2, inputBuffer.nWrites, inputBuffer.nPairs, outputBuffer.memIncr, this.outputsPerInput);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return true;
    }
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
        map(inputKeys[3], inputVals1[3], inputVals2[3]);
    }
    @Override
    public HadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException {
        Context ctx = (Context)context;
        this.javaProfile = new HadoopCLAccumulatedProfile();
        this.javaProfile.startOverall();
        while(ctx.nextKeyValue()) {
            this.javaProfile.startRead();
            IntWritable key = (IntWritable)ctx.getCurrentKey();
            PairWritable val = (PairWritable)ctx.getCurrentValue();
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            map(key.get()
, val.getVal1(), val.getVal2()
);
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

