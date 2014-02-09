package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;

public class RunnerDoneMarker extends HadoopCLOutputBuffer {
    public RunnerDoneMarker(HadoopOpenCLContext clContext) {
        super(clContext, -1);
    }

    public int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar)
        throws IOException, InterruptedException {
      return -1;
    }

    public void copyOverFromKernel(HadoopCLKernel kernel) { }
    public HashSet<Integer> constructIterSet() { return null; }
    public boolean completedAll() { return true; }

    public boolean isEnd() {
      return true;
    }

    @Override
    public Class<?> getOutputKeyClass() { return null; }
    @Override
    public Class<?> getOutputValClass() { return null; }
    @Override
    public int getPartitionFor(int index, int numReduceTasks) {
        throw new UnsupportedOperationException();
    }
    @Override
    public HadoopCLKeyValueIterator getKeyValueIterator(int soFar, int numReduceTasks) {
        throw new UnsupportedOperationException();
    }
}
