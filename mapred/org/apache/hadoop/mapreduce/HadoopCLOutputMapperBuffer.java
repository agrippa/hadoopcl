package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.util.HashSet;
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

    @Override
    public void copyOverFromKernel(HadoopCLKernel genericKernel) {
        HadoopCLMapperKernel kernel = (HadoopCLMapperKernel)genericKernel;
        this.itersFinished = constructIterSet();
        this.nPairs = kernel.nPairs;
        this.prof = kernel.openclProfile;
    }

    @Override
    public HashSet<Integer> constructIterSet() {
      HashSet<Integer> itersFinished = new HashSet<Integer>();
      for (int i = 0; i < this.nPairs; i++) {
        if (this.nWrites[i] >= 0) {
          itersFinished.add(i);
        }
      }
      return itersFinished;
    }

    @Override
    public long space() {
        return super.space();
    }
}
