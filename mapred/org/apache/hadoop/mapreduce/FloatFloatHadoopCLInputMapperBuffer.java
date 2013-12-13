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

public class FloatFloatHadoopCLInputMapperBuffer extends HadoopCLInputMapperBuffer {
    public float[] inputKeys;
    public float[] inputVals;
    protected int outputsPerInput;


    @Override
    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);

        inputKeys = new float[this.clContext.getBufferSize()];
        inputVals = new float[this.clContext.getBufferSize()];
        this.initialized = true;
    }

    @Override
    public void addTypedValue(Object val) {
        FloatWritable actual = (FloatWritable)val;
        this.inputVals[this.nPairs] = actual.get();
    }

    @Override
    public void addTypedKey(Object key) {
        FloatWritable actual = (FloatWritable)key;
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
            (inputKeys.length * 4) +
            (inputVals.length * 4);
    }

}

