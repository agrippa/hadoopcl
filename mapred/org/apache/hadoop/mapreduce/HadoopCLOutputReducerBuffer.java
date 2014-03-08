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
import java.util.List;
import java.util.ArrayList;

public abstract class HadoopCLOutputReducerBuffer extends HadoopCLOutputBuffer {
    public int nKeys;

    public HadoopCLOutputReducerBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
    }
    
    @Override
    public void copyOverFromKernel(HadoopCLKernel genericKernel) {
        HadoopCLReducerKernel kernel = (HadoopCLReducerKernel)genericKernel;
        this.nKeys = kernel.nKeys;
        this.itersFinished = constructIterSet();
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
