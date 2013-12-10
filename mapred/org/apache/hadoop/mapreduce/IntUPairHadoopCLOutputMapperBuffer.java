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

public class IntUPairHadoopCLOutputMapperBuffer extends HadoopCLOutputMapperBuffer {
    public int[] outputKeys;
    public int[] outputValIds;
    public double[] outputVals1;
    public double[] outputVals2;
    protected int outputsPerInput;
    private final int lockingInterval = 256;


    @Override
    public int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar) throws IOException, InterruptedException {
        final IntWritable saveKey = new IntWritable();
        final UniquePairWritable saveVal = new UniquePairWritable();
            if (this.memIncr[0] != 0) {
                int limit = this.outputKeys.length < this.memIncr[0] ? this.outputKeys.length : this.memIncr[0];
               for(int i = 0; i < limit; i++) {
                                saveKey.set(this.outputKeys[i]);
                                saveVal.set(this.outputValIds[i], this.outputVals1[i], this.outputVals2[i]);
                            context.write(saveKey, saveVal);
               }
            } else {
               if(isGPU == 0) {
                   for(int i = 0; i < this.nPairs; i++) {
                       for(int j = 0; j < this.nWrites[i]; j++) {
                                saveKey.set(this.outputKeys[i * this.outputsPerInput + j]);
                                saveVal.set(this.outputValIds[i * this.outputsPerInput + j], this.outputVals1[i * this.outputsPerInput + j], this.outputVals2[i * this.outputsPerInput + j]);
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
                                saveVal.set(this.outputValIds[base + i], this.outputVals1[base + i], this.outputVals2[base + i]);
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
            outputKeys = new int[this.clContext.getBufferSize() * 5];
            outputValIds = new int[this.clContext.getBufferSize() * 5];
            outputVals1 = new double[this.clContext.getBufferSize() * 5];
            outputVals2 = new double[this.clContext.getBufferSize() * 5];
        } else {
            outputKeys = new int[this.clContext.getBufferSize() * outputsPerInput];
            outputValIds = new int[this.clContext.getBufferSize() * outputsPerInput];
            outputVals1 = new double[this.clContext.getBufferSize() * outputsPerInput];
            outputVals2 = new double[this.clContext.getBufferSize() * outputsPerInput];
        }
        this.initialized = true;
    }

    @Override
    public long space() {
        return super.space() + 
            (outputKeys.length * 4) +
            (outputVals1.length * 8) +
            (outputVals2.length * 8) +
            (outputValIds.length * 4);
    }

}

