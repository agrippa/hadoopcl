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

public final class LongLongHadoopCLInputMapperBuffer extends HadoopCLInputMapperBuffer {
    public  long[] inputKeys;
    public  long[] inputVals;


    public LongLongHadoopCLInputMapperBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);

        inputKeys = new long[this.clContext.getInputBufferSize()];
        inputVals = new long[this.clContext.getInputBufferSize()];
    }


    @Override
    public final int bulkFill(HadoopCLDataInput stream) throws IOException {
        int nread = 0;
        while (stream.hasMore() &&
                this.nPairs < this.capacity &&
                true) {
            stream.nextKey();
            final long tmpKey = stream.readLong();
            stream.nextValue();
            final long tmpVal = stream.readLong();
            this.inputKeys[this.nPairs] = tmpKey;
            this.inputVals[this.nPairs] = tmpVal;
            this.nPairs++;
            nread++;
        }
        if (!(this.nPairs < this.capacity) ||
            !(true)) {
            this.isFull = true;
        }
        return nread;
    }
    @Override
    public final void addTypedValue(Object val) {
        LongWritable actual = (LongWritable)val;
        this.inputVals[this.nPairs] = actual.get();
    }

    @Override
    public final void addTypedKey(Object key) {
        LongWritable actual = (LongWritable)key;
        this.inputKeys[this.nPairs] = actual.get();
    }

    @Override
    public final boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {
        if (this.doingBulkRead) {
            return this.isFull;
        } else {
            return this.nPairs == this.capacity;
        }
    }

    @Override
    public void reset() {
        this.nPairs = 0;
        this.isFull = false;
    }

    @Override
    public long space() {
        return super.space() + 
            (inputKeys.length * 8) +
            (inputVals.length * 8);
    }

}

