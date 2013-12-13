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

public class LongLongHadoopCLInputMapperBuffer extends HadoopCLInputMapperBuffer {
    public long[] inputKeys;
    public long[] inputVals;
    protected int outputsPerInput;


    @Override
    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);

        inputKeys = new long[this.clContext.getBufferSize()];
        inputVals = new long[this.clContext.getBufferSize()];
        this.initialized = true;
    }

    @Override
    public void addTypedValue(Object val) {
        LongWritable actual = (LongWritable)val;
        this.inputVals[this.nPairs] = actual.get();
    }

    @Override
    public void addTypedKey(Object key) {
        LongWritable actual = (LongWritable)key;
        this.inputKeys[this.nPairs] = actual.get();
    }

    @Override
    public boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {
        return this.nPairs == this.capacity();
    }

    @Override
    public void reset() {
        this.nPairs = 0;
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
            (inputKeys.length * 8) +
            (inputVals.length * 8);
    }

}

