package org.apache.hadoop.mapreduce;

import java.util.HashSet;
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
        this.memRetry = new int[1];
        this.nWrites = new int[this.clContext.getBufferSize()];
    }

    @Override
    public boolean completedAll() {
        // int count = 0;
        // for (int i = 0; i < this.nKeys; i++) {
        //   if (nWrites[i] == -1) count++;
        // }
        // System.out.println("Did not complete "+count);
        for(int i = 0; i < this.nKeys; i++) {
            if(nWrites[i] == -1) return false;
        }
        return true;
    }

    @Override
    public void copyOverFromKernel(HadoopCLKernel genericKernel) {
        HadoopCLReducerKernel kernel = (HadoopCLReducerKernel)genericKernel;
        this.itersFinished = constructIterSet();
        this.nKeys = kernel.nKeys;
        this.prof = kernel.openclProfile;
    }

    @Override
    public HashSet<Integer> constructIterSet() {
      HashSet<Integer> itersFinished = new HashSet<Integer>();
      for (int i = 0; i < this.nKeys; i++) {
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
