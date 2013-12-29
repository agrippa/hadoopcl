package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.mapreduce.Reducer.Context;
import com.amd.aparapi.Range;
import com.amd.aparapi.Kernel;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import java.util.List;
import java.util.ArrayList;

public abstract class HadoopCLReducerKernel extends HadoopCLKernel {

    protected int deviceID;
    protected int isGPU;
    protected int[] input_keyIndex;
    protected int[] nWrites;
    protected int nKeys;
    protected int nVals;

    public void baseInit(HadoopOpenCLContext clContext) {
        int valuesPerKeyGuess = 16;
        this.clContext = clContext;
        this.deviceID = this.clContext.getDeviceId();
        this.isGPU = this.clContext.isGPU();
    }

    /* Type specific stuff */
    protected abstract void callReduce(int startOffset, int stopOffset);
    /**********************************/

    public int getInputValPerInputKey() {
        return 16;
    }

    public boolean launchKernel() throws IOException, InterruptedException {
        int globalSize = (this.nKeys + clContext.getThreadsPerGroup() - 1) / clContext.getThreadsPerGroup();
        globalSize *= clContext.getThreadsPerGroup();

        return this.execute(this.clContext.getDevice().createRange(globalSize, clContext.getThreadsPerGroup())) != null;
    }

    @Override
    public void run() {

        int start = -1;
        int increment = -1;
        int end = -1;

        if(isGPU == 0) {
            int chunkSize = (this.nKeys + getGlobalSize() - 1) / getGlobalSize();
            start = chunkSize * getGlobalId();
            end = chunkSize * (getGlobalId()+1);
            if(end > this.nKeys) end = this.nKeys;
            increment = 1;
        } else {
            start = getGlobalId();
            end = this.nKeys;
            increment = getGlobalSize();
        }

        for(int iter = start; iter < end && !outOfMemory(); iter += increment) {
            if (nWrites[iter] == -1) {
                nWrites[iter] = 0;
                int startOffset = input_keyIndex[iter];
                int stopOffset = nVals;
                if(iter < this.nKeys-1) {
                    stopOffset = input_keyIndex[iter+1];
                }
                callReduce(startOffset, stopOffset);
                if (outOfMemory()) iter = end;
            }
        }

    }
}
