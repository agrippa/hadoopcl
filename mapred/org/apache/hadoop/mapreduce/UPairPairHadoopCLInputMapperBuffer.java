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

public final class UPairPairHadoopCLInputMapperBuffer extends HadoopCLInputMapperBuffer {
    public  int[] inputKeyIds;
    public  double[] inputKeys1;
    public  double[] inputKeys2;
    public  double[] inputVals1;
    public  double[] inputVals2;


    public UPairPairHadoopCLInputMapperBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);

        inputKeyIds = new int[this.clContext.getInputBufferSize()];
        inputKeys1 = new double[this.clContext.getInputBufferSize()];
        inputKeys2 = new double[this.clContext.getInputBufferSize()];
        inputVals1 = new double[this.clContext.getInputBufferSize()];
        inputVals2 = new double[this.clContext.getInputBufferSize()];
    }


    @Override
    public final int bulkFill(HadoopCLDataInput stream) throws IOException {
        int nread = 0;
        while (stream.hasMore() &&
                this.nPairs < this.capacity &&
                true) {
            stream.nextKey();
            final int tmpKeyId = stream.readInt();
            final double tmpKey1 = stream.readDouble();
            final double tmpKey2 = stream.readDouble();
            stream.nextValue();
            final double tmpVal1 = stream.readDouble();
            final double tmpVal2 = stream.readDouble();
            this.inputKeyIds[this.nPairs] = tmpKeyId;
            this.inputKeys1[this.nPairs] = tmpKey1;
            this.inputKeys2[this.nPairs] = tmpKey2;
            this.inputVals1[this.nPairs] = tmpVal1;
            this.inputVals2[this.nPairs] = tmpVal2;
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
        PairWritable actual = (PairWritable)val;
        this.inputVals1[this.nPairs] = actual.getVal1();
        this.inputVals2[this.nPairs] = actual.getVal2();
    }

    @Override
    public final void addTypedKey(Object key) {
        UniquePairWritable actual = (UniquePairWritable)key;
        this.inputKeyIds[this.nPairs] = actual.getIVal();
        this.inputKeys1[this.nPairs] = actual.getVal1();
        this.inputKeys2[this.nPairs] = actual.getVal2();
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
            (inputKeys1.length * 8) +
            (inputKeys2.length * 8) +
            (inputKeyIds.length * 4) +
            (inputVals1.length * 8) +
            (inputVals2.length * 8);
    }

}

