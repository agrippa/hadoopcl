package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;

public class RunnerDoneMarker extends HadoopCLOutputBuffer {
    public void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext) {
    }

    public int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar)
        throws IOException, InterruptedException {
      return -1;
    }

    public void copyOverFromKernel(HadoopCLKernel kernel) { }
    public HashSet<Integer> constructIterSet() { return null; }

    public boolean isEnd() {
      return true;
    }
}
