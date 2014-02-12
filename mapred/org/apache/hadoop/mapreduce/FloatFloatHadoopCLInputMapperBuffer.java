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

public final class FloatFloatHadoopCLInputMapperBuffer extends HadoopCLInputMapperBuffer {
    public  float[] inputKeys;
    public  float[] inputVals;


    public FloatFloatHadoopCLInputMapperBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);

        inputKeys = new float[this.clContext.getInputBufferSize()];
        inputVals = new float[this.clContext.getInputBufferSize()];
    }


    @Override
    public final int bulkFill(HadoopCLDataInput stream) throws IOException {
        int nread = 0;
        while (stream.hasMore() &&
                this.nPairs < this.capacity &&
                true) {
            stream.nextKey();
            final float tmpKey = stream.readFloat();
            stream.nextValue();
            final float tmpVal = stream.readFloat();
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
        FloatWritable actual = (FloatWritable)val;
        this.inputVals[this.nPairs] = actual.get();
    }

    @Override
    public final void addTypedKey(Object key) {
        FloatWritable actual = (FloatWritable)key;
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
            (inputKeys.length * 4) +
            (inputVals.length * 4);
    }

}

