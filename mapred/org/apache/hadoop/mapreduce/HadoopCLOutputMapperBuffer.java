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

    public HadoopCLOutputMapperBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
    }

    @Override
    public boolean completedAll() {
        // StringBuffer sb = new StringBuffer();
        // int count = 0;
        // for (int i = 0; i < this.nPairs; i++) {
        //   sb.append(nWrites[i]+" ");
        //   if (nWrites[i] == -1) count++;
        // }
        // sb.insert(0, "nWrites ("+count+" not completed) ");
        // System.err.println(sb.toString());

        for(int i = 0; i < this.nPairs; i++) {
            if(nWrites[i] == -1) return false;
        }
        return true;
    }

    @Override
    public void copyOverFromKernel(HadoopCLKernel genericKernel) {
        HadoopCLMapperKernel kernel = (HadoopCLMapperKernel)genericKernel;
        this.nPairs = kernel.nPairs;
        this.itersFinished = constructIterSet();
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
