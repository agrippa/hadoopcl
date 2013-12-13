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

public class IntFsvecHadoopCLInputReducerBuffer extends HadoopCLInputReducerBuffer {
    public int[] inputKeys;
    public int[] inputValLookAsideBuffer;
    public int[] inputValIndices;
    public float[] inputValVals;
    protected int outputsPerInput;
    protected int individualInputValsCount;
    public int nVectorsToBuffer;


    @Override
    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);

        int inputValPerInputKey = this.getInputValPerInputKey();
        this.tempBuffer1 = new HadoopCLResizableIntArray();
        this.tempBuffer2 = new HadoopCLResizableIntArray();
        this.tempBuffer3 = new HadoopCLResizableFloatArray();

        inputKeys = new int[this.clContext.getBufferSize()];
        inputValLookAsideBuffer = new int[this.clContext.getBufferSize() * inputValPerInputKey];

        inputValIndices = new int[(this.clContext.getBufferSize() * inputValPerInputKey) * 5];

        inputValVals = new float[(this.clContext.getBufferSize() * inputValPerInputKey) * 5];

        this.individualInputValsCount = 0;
        this.nVectorsToBuffer = clContext.getNVectorsToBuffer();
        System.err.println("Setting nVectorsToBuffer to "+this.nVectorsToBuffer);
        this.initialized = true;
    }

    @Override
    public void bufferInputValue(Object obj) {
        FSparseVectorWritable actual = (FSparseVectorWritable)obj;
        ((HadoopCLResizableIntArray)this.tempBuffer1).add(this.tempBuffer2.size());
        for(int i = 0; i < actual.size(); i++) {
            ((HadoopCLResizableIntArray)this.tempBuffer2).add(actual.indices()[i]);
            ((HadoopCLResizableFloatArray)this.tempBuffer3).add(actual.vals()[i]);
        }
    }

    @Override
    public void useBufferedValues() {
        for(int i = 0; i < this.tempBuffer1.size(); i++) {
            this.inputValLookAsideBuffer[this.nVals + i] = this.individualInputValsCount + ((int[])this.tempBuffer1.getArray())[i];
        }
        System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputValIndices, this.individualInputValsCount, this.tempBuffer2.size());
        System.arraycopy(this.tempBuffer3.getArray(), 0, this.inputValVals, this.individualInputValsCount, this.tempBuffer3.size());
        this.individualInputValsCount += this.tempBuffer2.size();
        this.nVals += this.tempBuffer1.size();
    }
    @Override
    public void addTypedValue(Object val) {
        FSparseVectorWritable actual = (FSparseVectorWritable)val;
        this.inputValLookAsideBuffer[this.nVals] = this.individualInputValsCount;
        System.arraycopy(this.inputValIndices, this.individualInputValsCount, actual.indices(), 0, actual.size());
        System.arraycopy(this.inputValVals, this.individualInputValsCount, actual.vals(), 0, actual.size());
        this.individualInputValsCount += actual.size();
    }

    @Override
    public void addTypedKey(Object key) {
        IntWritable actual = (IntWritable)key;
        this.inputKeys[this.nKeys] = actual.get();
    }

    @Override
    public boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {
        Context reduceContext = (Context)context;
        tempBuffer1.reset();
        if(tempBuffer2 != null) tempBuffer2.reset();
        if(tempBuffer3 != null) tempBuffer3.reset();
        for(Object v : reduceContext.getValues()) {
            bufferInputValue(v);
        }
        return (this.nKeys == this.inputKeys.length || this.individualInputValsCount + this.tempBuffer2.size() > this.inputValIndices.length);
    }

    @Override
    public void reset() {
        this.nKeys = 0;
        this.nVals = 0;
        this.individualInputValsCount = 0;
        this.maxInputValsPerInputKey = 0;
    }

    @Override
    public void transferBufferedValues(HadoopCLBuffer buffer) {
        this.tempBuffer1.copyTo(((HadoopCLInputReducerBuffer)buffer).tempBuffer1);
        if(this.tempBuffer2 != null) this.tempBuffer2.copyTo(((HadoopCLInputReducerBuffer)buffer).tempBuffer2);
        if(this.tempBuffer3 != null) this.tempBuffer3.copyTo(((HadoopCLInputReducerBuffer)buffer).tempBuffer3);
    }
    @Override
    public void resetForAnotherAttempt() {
        // NO-OP at the moment, but might be necessary later
    }

    @Override
    public long space() {
        return super.space() + 
            (inputKeys.length * 4) +
            (inputValLookAsideBuffer.length * 4) +
            (inputValIndices.length * 4) +
            (inputValVals.length * 8);
    }

}

