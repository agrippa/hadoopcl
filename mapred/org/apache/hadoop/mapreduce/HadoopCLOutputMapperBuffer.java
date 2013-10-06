package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.lang.InterruptedException;
import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;
import java.util.HashMap;

public abstract class HadoopCLOutputMapperBuffer extends HadoopCLOutputBuffer {
    public int nPairs;

    public void baseInit(HadoopOpenCLContext clContext) {
        this.clContext = clContext;
        this.isGPU = this.clContext.isGPU();
        this.memIncr = new int[1];
        this.nWrites = new int[this.clContext.getBufferSize()];
    }

    public void copyOverFromInput(HadoopCLInputBuffer inputBuffer) {
        this.nPairs = ((HadoopCLInputMapperBuffer)inputBuffer).nPairs;
        System.arraycopy(inputBuffer.nWrites, 0, this.nWrites, 0, this.nWrites.length);
        this.prof = inputBuffer.prof;
    }

    @Override
    public long space() {
        return super.space();
    }
}
