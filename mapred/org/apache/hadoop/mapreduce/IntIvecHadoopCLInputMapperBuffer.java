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

public class IntIvecHadoopCLInputMapperBuffer extends HadoopCLInputMapperBuffer {
    public int[] inputKeys;
    public int[] inputValLookAsideBuffer;
    public HadoopCLResizableIntArray inputVal;
    protected int outputsPerInput;
    protected int individualInputValsCount;
    public TreeMap<Integer, LinkedList<IndValWrapper>> sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();
    public int nVectorsToBuffer;


    @Override
    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);

        inputKeys = new int[this.clContext.getBufferSize()];
        inputValLookAsideBuffer = new int[this.clContext.getBufferSize()];

        inputVal = new HadoopCLResizableIntArray((this.clContext.getBufferSize()) * 5);

        this.individualInputValsCount = 0;
        this.nVectorsToBuffer = clContext.getNVectorsToBuffer();
        System.err.println("Setting nVectorsToBuffer to "+this.nVectorsToBuffer);
        this.initialized = true;
    }

    @Override
    public void addTypedValue(Object val) {
        IntegerVectorWritable actual = (IntegerVectorWritable)val;
        this.inputValLookAsideBuffer[this.nPairs] = this.individualInputValsCount;
        if (this.enableStriding) {
            IndValWrapper wrapper = new IndValWrapper(actual.vals(), actual.size());
            if (this.sortedVals.containsKey(actual.size())) {
                this.sortedVals.get(actual.size()).add(wrapper);
            } else {
                LinkedList<IndValWrapper> newList = new LinkedList<IndValWrapper>();
                newList.add(wrapper);
                this.sortedVals.put(actual.size(), newList);
            }
        } else {
            this.inputVal.ensureCapacity(this.individualInputValsCount + actual.size());
            System.arraycopy(actual.vals(), 0, this.inputVal.getArray(), this.individualInputValsCount, actual.size());
        }
        this.individualInputValsCount += actual.size();
    }

    @Override
    public void addTypedKey(Object key) {
        IntWritable actual = (IntWritable)key;
        this.inputKeys[this.nPairs] = actual.get();
    }

    @Override
    public boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {
        IntegerVectorWritable curr = (IntegerVectorWritable)((Context)context).getCurrentValue();
        if (this.enableStriding) {
            return this.nPairs == this.capacity() || this.nPairs == nVectorsToBuffer;
        } else {
            return this.nPairs == this.capacity() || this.individualInputValsCount + curr.size() > this.inputVal.length();
        }
    }

    @Override
    public void reset() {
        this.nPairs = 0;
        this.individualInputValsCount = 0;
        this.inputVal.reset();
        this.sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();
    }

    @Override
    public void transferBufferedValues(HadoopCLBuffer buffer) {
        // NOOP
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
            (inputVal.length() * 4);
    }

}

