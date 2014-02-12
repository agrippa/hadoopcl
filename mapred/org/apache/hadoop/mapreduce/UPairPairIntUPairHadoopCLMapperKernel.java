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

public abstract class UPairPairIntUPairHadoopCLMapperKernel extends HadoopCLMapperKernel {
    public int outputLength;


    public  int[] inputKeyIds;
    public  double[] inputKeys1;
    public  double[] inputKeys2;
    public  double[] inputVals1;
    public  double[] inputVals2;
    public  int[] outputKeys;
    public  int[] outputValIds;
    public  double[] outputVals1;
    public  double[] outputVals2;

    protected abstract void map(int keyId, double key1, double key2, double val1, double val2);

    public void postKernelSetup(int[] setOutputkeys, int[] setOutputvalIds, double[] setOutputvals1, double[] setOutputvals2, int[] setMemIncr, int[] setNWrites, int[] outputIterMarkers) {
        this.outputKeys = setOutputkeys;
        this.outputValIds = setOutputvalIds;
        this.outputVals1 = setOutputvals1;
        this.outputVals2 = setOutputvals2;

        this.memIncr = setMemIncr;
        this.nWrites = setNWrites;

        this.outputIterMarkers = outputIterMarkers;
    }

    public void preKernelSetup(int[] setInputkeyIds, double[] setInputkeys1, double[] setInputkeys2, double[] setInputvals1, double[] setInputvals2, int[] setNWrites, int setNPairs) {
        this.inputKeyIds = setInputkeyIds;
        this.inputKeys1 = setInputkeys1;
        this.inputKeys2 = setInputkeys2;
        this.inputVals1 = setInputvals1;
        this.inputVals2 = setInputvals2;
        this.outputKeys = null;
        this.outputValIds = null;
        this.outputVals1 = null;
        this.outputVals2 = null;
        this.nWrites = setNWrites;
        this.nPairs = setNPairs;

        this.memIncr = null;

        this.outputLength = this.getArrayLength("outputValIds");
        this.outputIterMarkers = null;
    }

    public UPairPairIntUPairHadoopCLMapperKernel() {
        super(null, null);
        throw new UnsupportedOperationException();
    }
    public UPairPairIntUPairHadoopCLMapperKernel(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.setStrided(false);

        this.arrayLengths.put("outputIterMarkers", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("memIncr", 1);
        this.arrayLengths.put("outputKeys", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputValIds", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputVals1", this.clContext.getOutputBufferSize());
        this.arrayLengths.put("outputVals2", this.clContext.getOutputBufferSize());
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return UPairPairHadoopCLInputMapperBuffer.class; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return IntUPairHadoopCLOutputMapperBuffer.class; }

    @Override
    public void fill(HadoopCLInputBuffer genericInputBuffer) {
        UPairPairHadoopCLInputMapperBuffer inputBuffer = (UPairPairHadoopCLInputMapperBuffer)genericInputBuffer;
        this.preKernelSetup(inputBuffer.inputKeyIds, inputBuffer.inputKeys1, inputBuffer.inputKeys2, inputBuffer.inputVals1, inputBuffer.inputVals2, inputBuffer.nWrites, inputBuffer.nPairs);
    }

    @Override
    public void prepareForRead(HadoopCLOutputBuffer genericOutputBuffer) {
        IntUPairHadoopCLOutputMapperBuffer outputBuffer = (IntUPairHadoopCLOutputMapperBuffer)genericOutputBuffer;
        this.postKernelSetup(outputBuffer.outputKeys, outputBuffer.outputValIds, outputBuffer.outputVals1, outputBuffer.outputVals2, outputBuffer.memIncr, outputBuffer.nWrites, outputBuffer.outputIterMarkers);
    }

    @Override
    public boolean equalInputOutputTypes() {
        return false;
    }

    private final IntWritable keyObj = new IntWritable();
    private final UniquePairWritable valObj = new UniquePairWritable();

    protected boolean write(int key, int valId, double val1, double val2) {
        this.javaProfile.stopKernel();
        this.javaProfile.startWrite();
        keyObj.set(key);
        valObj.set(valId, val1, val2);
        try { clContext.getContext().write(keyObj, valObj); } catch(Exception ex) { throw new RuntimeException(ex); }
        this.javaProfile.stopWrite();
        this.javaProfile.startKernel();
        return true;
    }

    @Override
    protected void callMap() {
        map(inputKeyIds[3], inputKeys1[3], inputKeys2[3], inputVals1[3], inputVals2[3]);
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
            UniquePairWritable key = (UniquePairWritable)ctx.getCurrentKey();
            PairWritable val = (PairWritable)ctx.getCurrentValue();
            this.javaProfile.stopRead();
            this.javaProfile.startKernel();
            map(key.getIVal(), key.getVal1(), key.getVal2(), val.getVal1(), val.getVal2()
);
            this.javaProfile.stopKernel();
            OpenCLDriver.inputsRead++;
        }
        this.javaProfile.stopOverall();
        return this.javaProfile;
    }
}

