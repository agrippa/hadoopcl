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

    protected final int deviceID;
    public final int isGPU;
    public int[] input_keyIndex;
    public int[] nWrites;
    public int nKeys;
    public int nVals;

    public HadoopCLReducerKernel(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.deviceID = this.clContext.getDeviceId();
        this.isGPU = this.clContext.isGPU();
    }

    /* Type specific stuff */
    protected abstract void callReduce(int startOffset, int stopOffset);
    /**********************************/

    @Override
    public boolean relaunchKernel() throws IOException, InterruptedException {
        int globalSize = (this.nKeys + clContext.getThreadsPerGroup() - 1) /
            clContext.getThreadsPerGroup();
        globalSize *= clContext.getThreadsPerGroup();

        return this.reExecute(this.clContext.getDevice().createRange(globalSize,
                    clContext.getThreadsPerGroup()), this.clContext.getDeviceId(), this.clContext.getDeviceSlot(),
            this.clContext.verboseTypeName()+"::"+this.tracker.toString(), this.clContext.getNKernels()) != null;
    }

    @Override
    public boolean launchKernel() throws IOException, InterruptedException {
        int globalSize = (this.nKeys + clContext.getThreadsPerGroup() - 1) /
            clContext.getThreadsPerGroup();
        globalSize *= clContext.getThreadsPerGroup();

        return this.execute(this.clContext.getDevice().createRange(globalSize,
                    clContext.getThreadsPerGroup()), this.clContext.getDeviceId(), this.clContext.getDeviceSlot(),
            this.clContext.verboseTypeName()+"::"+this.tracker.toString(), this.clContext.getNKernels()) != null;
    }

    @Override
    public void run() {

        int anyRestartRequired = 0;
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

        for(int iter = start; iter < end; iter += increment) {
            if (nWrites[iter] == -1) {
                nWrites[iter] = 0;
                int startOffset = input_keyIndex[iter];
                int stopOffset = nVals;
                if(iter < this.nKeys-1) {
                    stopOffset = input_keyIndex[iter+1];
                }
                callReduce(startOffset, stopOffset);
                if (nWrites[iter] < 0) {
                    anyRestartRequired = 1;
                    iter = end;
                }
            }
        }
        if (anyRestartRequired == 1) {
            memWillRequireRestart[0] = 1;
        }
    }
}
