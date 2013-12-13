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

public class LongLongHadoopCLOutputMapperBuffer extends HadoopCLOutputMapperBuffer {
    public long[] outputKeys;
    public long[] outputVals;
    protected int outputsPerInput;
    private final int lockingInterval = 256;


    @Override
    public int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar) throws IOException, InterruptedException {
        final LongWritable saveKey = new LongWritable();
        final LongWritable saveVal = new LongWritable();
            if (this.memIncr[0] != 0) {
                int limit = this.outputKeys.length < this.memIncr[0] ? this.outputKeys.length : this.memIncr[0];
               for(int i = 0; i < limit; i++) {
                                saveKey.set(this.outputKeys[i]);
                                saveVal.set(this.outputVals[i]);
                            context.write(saveKey, saveVal);
               }
            } else {
               if(isGPU == 0) {
                   for(int i = 0; i < this.nPairs; i++) {
                       for(int j = 0; j < this.nWrites[i]; j++) {
                                saveKey.set(this.outputKeys[i * this.outputsPerInput + j]);
                                saveVal.set(this.outputVals[i * this.outputsPerInput + j]);
                            context.write(saveKey, saveVal);
                       }
                   }
               } else {
                   int j = 0;
                   boolean someLeft = false;
                   int base = 0;
                   do {
                       someLeft = false;
                       for(int i = 0; i < this.nPairs; i++) {
                           if(this.nWrites[i] > j) {
                                saveKey.set(this.outputKeys[base + i]);
                                saveVal.set(this.outputVals[base + i]);
                                    context.write(saveKey, saveVal);
                               if(this.nWrites[i] > j + 1) someLeft = true;
                           }
                       }
                       base += this.nPairs;
                       j++;
                   } while(someLeft);
               }
            }
            return -1;
    }

    @Override
    public void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);
        this.outputsPerInput = outputsPerInput;

        if (this.outputsPerInput < 0) {
            outputKeys = new long[this.clContext.getBufferSize() * 5];
            outputVals = new long[this.clContext.getBufferSize() * 5];
        } else {
            outputKeys = new long[this.clContext.getBufferSize() * outputsPerInput];
            outputVals = new long[this.clContext.getBufferSize() * outputsPerInput];
        }
        this.initialized = true;
    }

    @Override
    public long space() {
        return super.space() + 
            (outputKeys.length * 8) +
            (outputVals.length * 8);
    }

}

