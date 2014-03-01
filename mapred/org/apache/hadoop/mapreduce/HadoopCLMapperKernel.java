
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.lang.InterruptedException;
import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;

public abstract class HadoopCLMapperKernel extends HadoopCLKernel {
    protected final int deviceID;
    public final int isGPU;
    public int[] nWrites;
    public int nPairs;

    public HadoopCLMapperKernel(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.isGPU = this.clContext.isGPU();
        this.deviceID = this.clContext.getDeviceId();
    }

    /* Type specific stuff */
    protected abstract void callMap();
    /**********************************/

    @Override
    public boolean launchKernel() throws IOException, InterruptedException {
        int globalSize = 1;
        // int globalSize = (this.nPairs + clContext.getThreadsPerGroup() - 1) / clContext.getThreadsPerGroup();
        globalSize *= clContext.getThreadsPerGroup();

        return this.execute(this.clContext.getDevice().createRange(globalSize, clContext.getThreadsPerGroup()),
            this.clContext.verboseTypeName()+"::"+this.tracker.toString()) != null;
    }

    @Override
    public boolean relaunchKernel() throws IOException, InterruptedException {
        int globalSize = 1;
        // int globalSize = (this.nPairs + clContext.getThreadsPerGroup() - 1) / clContext.getThreadsPerGroup();
        globalSize *= clContext.getThreadsPerGroup();

        return this.reExecute(this.clContext.getDevice().createRange(globalSize, clContext.getThreadsPerGroup()),
            this.clContext.verboseTypeName()+"::"+this.tracker.toString()) != null;
    }

    @Override
    public void run() {
        int anyRestartRequired = 0;
        int start = -1;
        int end = -1;
        int increment = -1;
        if(isGPU == 0) {
            int chunkSize = (this.nPairs + getGlobalSize() - 1) / getGlobalSize();
            start = chunkSize * getGlobalId();
            end = chunkSize * (getGlobalId()+1);
            if(end > this.nPairs) end = this.nPairs;
            increment = 1;
        } else {
            start = getGlobalId();
            end = this.nPairs;
            increment = getGlobalSize();
        }

        for(int iter = start; iter < end; iter = iter + increment) {
            if (nWrites[iter] == -1) {
                nWrites[iter] = 0;
                callMap();
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
