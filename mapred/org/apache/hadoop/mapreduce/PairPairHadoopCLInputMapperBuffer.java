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

public class PairPairHadoopCLInputMapperBuffer extends HadoopCLInputMapperBuffer {
    public double[] inputKeys1;
    public double[] inputKeys2;
    public double[] inputVals1;
    public double[] inputVals2;
    protected int outputsPerInput;


    @Override
    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);

        inputKeys1 = new double[this.clContext.getBufferSize()];
        inputKeys2 = new double[this.clContext.getBufferSize()];
        inputVals1 = new double[this.clContext.getBufferSize()];
        inputVals2 = new double[this.clContext.getBufferSize()];
        this.initialized = true;
    }

    @Override
    public void addTypedValue(Object val) {
        PairWritable actual = (PairWritable)val;
        this.inputVals1[this.nPairs] = actual.getVal1();
        this.inputVals2[this.nPairs] = actual.getVal2();
    }

    @Override
    public void addTypedKey(Object key) {
        PairWritable actual = (PairWritable)key;
        this.inputKeys1[this.nPairs] = actual.getVal1();
        this.inputKeys2[this.nPairs] = actual.getVal2();
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
            (inputKeys1.length * 8) +
            (inputKeys2.length * 8) +
            (inputVals1.length * 8) +
            (inputVals2.length * 8);
    }

}

