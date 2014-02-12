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

public final class IntSvecHadoopCLInputMapperBuffer extends HadoopCLInputMapperBuffer {
    public  int[] inputKeys;
    public  int[] inputValLookAsideBuffer;
    public  int[] inputValIndices;
    public  double[] inputValVals;
    public int individualInputValsCount;
    public int lastIndividualInputValsCount;
    public TreeMap<Integer, LinkedList<IndValWrapper>> sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();
    public int nVectorsToBuffer;


    public IntSvecHadoopCLInputMapperBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);

        inputKeys = new int[this.clContext.getInputBufferSize()];
        inputValLookAsideBuffer = new int[(this.clContext.getInputBufferSize())];

        inputValIndices = new int[(this.clContext.getInputBufferSize()) * this.clContext.getInputValEleMultiplier()];

        inputValVals = new double[(this.clContext.getInputBufferSize()) * this.clContext.getInputValEleMultiplier()];

        this.individualInputValsCount = 0;
        this.nVectorsToBuffer = clContext.getNVectorsToBuffer();
    }


    @Override
    public final int bulkFill(HadoopCLDataInput stream) throws IOException {
        int nread = 0;
        if (this.enableStriding) {
            while (stream.hasMore() &&
                    this.nPairs < this.capacity &&
                    this.nPairs < nVectorsToBuffer) {
                stream.nextKey();
                final int tmpKey = stream.readInt();
                stream.nextValue();
                final int vectorLength = stream.readInt();
                final int[] indices = new int[vectorLength];
                final double[] vals = new double[vectorLength];
                stream.readFully(indices, 0, vectorLength);
                stream.readFully(vals, 0, vectorLength);
                IndValWrapper wrapper = new IndValWrapper(indices, vals, vectorLength);
                if (this.sortedVals.containsKey(vectorLength)) {
                    this.sortedVals.get(vectorLength).add(wrapper);
                } else {
                    LinkedList<IndValWrapper> newList = new LinkedList<IndValWrapper>();
                    newList.add(wrapper);
                    this.sortedVals.put(vectorLength, newList);
                }
                this.individualInputValsCount += vectorLength;
                nread++;
            }
            if (!(this.nPairs < this.capacity) ||
                    !(this.nPairs < nVectorsToBuffer)) {
                this.isFull = true;
            }
        } else {
            while (stream.hasMore() &&
                    this.nPairs < this.capacity &&
                    this.nPairs < this.inputValLookAsideBuffer.length) {
                stream.nextKey();
                final int tmpKey = stream.readInt();
                stream.nextValue();
                final int vectorLength = stream.readInt();
                if (this.individualInputValsCount + vectorLength > this.inputValIndices.length) {
                    stream.prev();
                    this.isFull = true;
                    return nread;
                }
                this.inputKeys[this.nPairs] = tmpKey;
                this.inputValLookAsideBuffer[this.nPairs++] = this.individualInputValsCount;
                stream.readFully(this.inputValIndices, this.individualInputValsCount, vectorLength);
                stream.readFully(this.inputValVals, this.individualInputValsCount, vectorLength);
                this.individualInputValsCount += vectorLength;
                nread++;
            }
            if (!(this.nPairs < this.capacity) ||
                !(this.nPairs < this.inputValLookAsideBuffer.length)) {
                this.isFull = true;
            }
        }
        return nread;
    }
    @Override
    public final void addTypedValue(Object val) {
        SparseVectorWritable actual = (SparseVectorWritable)val;
        this.inputValLookAsideBuffer[this.nPairs] = this.individualInputValsCount;
        if (this.enableStriding) {
            IndValWrapper wrapper = new IndValWrapper(actual.indices(), actual.vals(), actual.size());
            if (this.sortedVals.containsKey(actual.size())) {
                this.sortedVals.get(actual.size()).add(wrapper);
            } else {
                LinkedList<IndValWrapper> newList = new LinkedList<IndValWrapper>();
                newList.add(wrapper);
                this.sortedVals.put(actual.size(), newList);
            }
        } else {
            System.arraycopy(actual.indices(), 0, this.inputValIndices, this.individualInputValsCount, actual.size());
            System.arraycopy(actual.vals(), 0, this.inputValVals, this.individualInputValsCount, actual.size());
        }
        this.individualInputValsCount += actual.size();
    }

    @Override
    public final void addTypedKey(Object key) {
        IntWritable actual = (IntWritable)key;
        this.inputKeys[this.nPairs] = actual.get();
    }

    @Override
    public final boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {
        if (this.doingBulkRead) {
            return this.isFull;
        } else {
            if (this.enableStriding) {
                return this.nPairs == this.capacity || this.nPairs == nVectorsToBuffer;
            } else {
                return this.nPairs == this.capacity || this.individualInputValsCount +
                    ((SparseVectorWritable)((Context)context).getCurrentValue()).size() > this.inputValIndices.length;
            }
        }
    }

    @Override
    public void reset() {
        this.nPairs = 0;
        this.individualInputValsCount = 0;
        this.sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();
        this.isFull = false;
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

