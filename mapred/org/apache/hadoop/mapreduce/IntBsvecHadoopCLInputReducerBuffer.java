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

public final class IntBsvecHadoopCLInputReducerBuffer extends HadoopCLInputReducerBuffer {
    public  int[] inputKeys;
    public  int[] inputValLookAsideBuffer;
    public  int[] inputValIndices;
    public  double[] inputValVals;
    private IntWritable currentKey;
    public int individualInputValsCount;
    public int lastIndividualInputValsCount;
    public int nVectorsToBuffer;


    public IntBsvecHadoopCLInputReducerBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);

        inputKeys = new int[this.clContext.getInputBufferSize()];
        inputValLookAsideBuffer = new int[(this.clContext.getInputBufferSize()) * this.clContext.getInputValMultiplier()];

        inputValIndices = new int[(this.clContext.getInputBufferSize()) * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];

        inputValVals = new double[(this.clContext.getInputBufferSize()) * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];

        this.individualInputValsCount = 0;
        this.nVectorsToBuffer = clContext.getNVectorsToBuffer();
    }


    @Override
    public final int bulkFill(HadoopCLDataInput stream) throws IOException {
        int nread = 0;
        while (stream.hasMore() &&
                this.nKeys < this.inputKeys.length &&
                this.nVals < this.inputValLookAsideBuffer.length) {
            stream.nextKey();
            final int tmpKey = stream.readInt();
            stream.nextValue();
            final int vectorLength = stream.readInt();
            if (this.individualInputValsCount + vectorLength > this.inputValIndices.length) {
                stream.prev();
                this.isFull = true;
                return nread;
            }
            if (this.currentKey == null || this.currentKey.get() != tmpKey) {
                this.keyIndex[this.nKeys] = this.nVals;
                this.inputKeys[this.nKeys] = tmpKey;
                this.nKeys++;
                if (this.currentKey == null) {
                    this.currentKey = new IntWritable(tmpKey);
                } else {
                    this.currentKey.set(tmpKey);
                }
            }
            this.inputValLookAsideBuffer[this.nVals++] = this.individualInputValsCount;
            stream.readFully(this.inputValIndices, this.individualInputValsCount, vectorLength);
            stream.readFully(this.inputValVals, this.individualInputValsCount, vectorLength);
            this.individualInputValsCount += vectorLength;
            nread++;
        }
        if (!(this.nKeys < this.inputKeys.length) ||
            !(this.nVals < this.inputValLookAsideBuffer.length)) {
            this.isFull = true;
        }
        return nread;
    }
    @Override
    public final void addTypedValue(Object val) {
        BSparseVectorWritable actual = (BSparseVectorWritable)val;
        this.inputValLookAsideBuffer[this.nVals++] = this.individualInputValsCount;
        System.arraycopy(actual.indices(), 0, this.inputValIndices, this.individualInputValsCount, actual.size());
        System.arraycopy(actual.vals(), 0, this.inputValVals, this.individualInputValsCount, actual.size());
        this.individualInputValsCount += actual.size();
    }

    @Override
    public final void addTypedKey(Object key) {
        IntWritable actual = (IntWritable)key;
        if (this.currentKey == null || !this.currentKey.equals(actual)) {
            this.keyIndex[this.nKeys] = this.nVals;
            this.inputKeys[this.nKeys] = actual.get();
            this.nKeys++;
            this.currentKey = actual.clone();
        }
    }

    @Override
    public final boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {
        if (this.doingBulkRead) {
            return this.isFull;
        } else {
            Context reduceContext = (Context)context;
            return (this.nKeys == this.inputKeys.length ||
                this.nVals == this.inputValLookAsideBuffer.length ||
                this.individualInputValsCount + ((BSparseVectorWritable)reduceContext.getCurrentValue()).size() > this.inputValIndices.length);
        }
    }

    @Override
    public void reset() {
        this.nKeys = 0;
        this.nVals = 0;
        this.lastNKeys = -1;
        this.lastNVals = -1;
        this.individualInputValsCount = 0;
        this.lastIndividualInputValsCount = -1;
        this.currentKey = null;
        this.isFull = false;
    }

    @Override
    public boolean sameAsLastKey(Object obj) {
        return ((IntWritable)obj).get() == this.inputKeys[this.nKeys-1];
    }
    @Override
    public void removeLastKey() {
        this.lastNKeys = this.nKeys;
        this.lastNVals = this.nVals;
        this.lastIndividualInputValsCount = this.individualInputValsCount;
        this.nKeys = this.nKeys - 1;
        this.nVals = this.keyIndex[this.nKeys];
        this.individualInputValsCount = this.inputValLookAsideBuffer[this.keyIndex[this.nKeys]];
    }
    @Override
    public void transferLastKey(HadoopCLInputReducerBuffer otherBuffer) {
        final IntBsvecHadoopCLInputReducerBuffer other = (IntBsvecHadoopCLInputReducerBuffer)otherBuffer;
        this.nKeys = 1;
        this.nVals = other.lastNVals - other.nVals;
        this.individualInputValsCount = other.lastIndividualInputValsCount - other.individualInputValsCount;
        this.inputKeys[0] = other.inputKeys[other.lastNKeys - 1];
        safeTransfer(other.inputValIndices, this.inputValIndices, other.individualInputValsCount, this.individualInputValsCount);
        safeTransfer(other.inputValVals,    this.inputValVals,    other.individualInputValsCount, this.individualInputValsCount);
        for (int i = 0; i < this.nVals; i++) {
            this.inputValLookAsideBuffer[i] = other.inputValLookAsideBuffer[other.nVals + i] - other.individualInputValsCount;
        }
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

