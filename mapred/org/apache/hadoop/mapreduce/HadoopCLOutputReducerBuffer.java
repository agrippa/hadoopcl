package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.mapreduce.Reducer.Context;
import com.amd.aparapi.Range;
import com.amd.aparapi.Kernel;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.HadoopCLResizableArray;
import java.util.List;
import java.util.ArrayList;

public abstract class HadoopCLOutputReducerBuffer extends HadoopCLOutputBuffer {
    public int nKeys;

    public void baseInit(HadoopOpenCLContext clContext) {
        this.clContext = clContext;
        this.isGPU = this.clContext.isGPU();
        this.memIncr = new int[1];
        this.resetProfile();
        this.nWrites = new int[this.clContext.getBufferSize()];
    }

    public void copyOverFromInput(HadoopCLInputBuffer inputBuffer) {
        this.nKeys = ((HadoopCLInputReducerBuffer)inputBuffer).nKeys;
        System.arraycopy(inputBuffer.nWrites, 0, this.nWrites, 0, this.nWrites.length);
        this.prof = inputBuffer.prof;
    }

    @Override
    public long space() {
        return super.space();
    }
}
