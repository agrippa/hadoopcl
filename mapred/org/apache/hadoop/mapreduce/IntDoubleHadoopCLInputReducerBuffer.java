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

public final class IntDoubleHadoopCLInputReducerBuffer extends HadoopCLInputReducerBuffer {
    public  int[] inputKeys;
    public  double[] inputVals;
    private IntWritable currentKey;


    public IntDoubleHadoopCLInputReducerBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);

        inputKeys = new int[this.clContext.getInputBufferSize()];
        inputVals = new double[this.clContext.getInputBufferSize()];
    }


    @Override
    public final int bulkFill(HadoopCLDataInput stream) throws IOException {
        int nread = 0;
        while (stream.hasMore() &&
                this.nKeys < this.inputKeys.length &&
                this.nVals < this.inputVals.length) {
            stream.nextKey();
            final int tmpKey = stream.readInt();
            stream.nextValue();
            final double tmpVal = stream.readDouble();
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
            this.inputVals[this.nVals] = tmpVal;
            this.nVals++;
            nread++;
        }
        if (!(this.nKeys < this.inputKeys.length) ||
            !(this.nVals < this.inputVals.length)) {
            this.isFull = true;
        }
        return nread;
    }
    @Override
    public final void addTypedValue(Object val) {
        DoubleWritable actual = (DoubleWritable)val;
        this.inputVals[this.nVals++] = actual.get();
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
            return (this.nKeys == this.inputKeys.length || this.nVals == this.inputVals.length);
        }
    }

    @Override
    public void reset() {
        this.nKeys = 0;
        this.nVals = 0;
        this.lastNKeys = -1;
        this.lastNVals = -1;
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
        this.nKeys = this.nKeys - 1;
        this.nVals = this.keyIndex[this.nKeys];
    }
    @Override
    public void transferLastKey(HadoopCLInputReducerBuffer otherBuffer) {
        final IntDoubleHadoopCLInputReducerBuffer other = (IntDoubleHadoopCLInputReducerBuffer)otherBuffer;
        this.nKeys = 1;
        this.nVals = other.lastNVals - other.nVals;
        this.inputKeys[0] = other.inputKeys[other.lastNKeys - 1];
        safeTransfer(other.inputVals, this.inputVals, other.nVals, this.nVals);
    }
    @Override
    public long space() {
        return super.space() + 
            (inputKeys.length * 4) +
            (inputVals.length * 8);
    }

}

