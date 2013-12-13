package org.apache.hadoop.mapreduce;

import java.io.IOException;

public class RunnerDoneMarker extends HadoopCLOutputBuffer {
    public void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext) {
    }

    public int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar)
        throws IOException, InterruptedException {
      return -1;
    }

    public void copyOverFromInput(HadoopCLInputBuffer inputBuffer) {
    }

    public boolean isEnd() {
      return true;
    }
}
