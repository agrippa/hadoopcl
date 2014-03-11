package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.HashSet;

public class CopyThreadDone extends HadoopCLOutputBuffer {
    public CopyThreadDone(HadoopOpenCLContext context, int id) {
        super(context, id);
    }

    public int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar)
            throws IOException, InterruptedException {
        return -1;
    }

    public Class<?> getOutputKeyClass() { return null; }
    public Class<?> getOutputValClass() { return null; }
    public void copyOverFromKernel(HadoopCLKernel kernel) { }
    public HashSet<Integer> constructIterSet() { return null; }
    // public HadoopCLKeyValueIterator getKeyValueIterator(List<OutputBufferSoFar> toWrite, int numReduceTasks) { return null; }
    public int getPartitionFor(int index, int numReduceTasks) { return -1; }
    public int getCount() { return -1; }

    public boolean completedAll() { return true; }
    public void printContents() { }
}
