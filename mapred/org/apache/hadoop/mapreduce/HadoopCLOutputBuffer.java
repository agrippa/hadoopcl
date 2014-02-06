package org.apache.hadoop.mapreduce;

import java.util.Iterator;
import java.util.HashSet;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.io.ReadArrayUtils;

public abstract class HadoopCLOutputBuffer extends HadoopCLBuffer {
    public int[] memIncr;
    protected int[] outputIterMarkers;
    public HashSet<Integer> itersFinished;
    protected final ReadArrayUtils readUtils = new ReadArrayUtils();

    public abstract void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext);
    public abstract int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar)
        throws IOException, InterruptedException;

    public abstract Class<?> getOutputKeyClass();
    public abstract Class<?> getOutputValClass();
    public abstract void copyOverFromKernel(HadoopCLKernel kernel);
    public abstract HashSet<Integer> constructIterSet();
    public abstract HadoopCLKeyValueIterator getKeyValueIterator(int soFar, int numReduceTasks);
    public abstract int getPartitionFor(int index, int numReduceTasks);

    @Override
    public long space() {
        return super.space() + (4 * memIncr.length);
    }

    class OutputIterator implements Iterator<Integer> {
        private final int start;
        private final int end;
        private final HashSet<Integer> valid;
        private final int[] markers;
        private int curr;

        public OutputIterator(int start, int end, HashSet<Integer> valid,
                int[] markers) {
            this.start = start;
            this.end = end;
            this.valid = valid;
            this.markers = markers;
            this.curr = this.start - 1;
            seekToNextValid();
        }

        private void seekToNextValid() {
            int newCurr = this.curr + 1;
            while (newCurr < this.end && !this.valid.contains(this.markers[newCurr])) {
                newCurr++;
            }
            this.curr = newCurr;
        }

        @Override
        public boolean hasNext() {
            return curr < end;
        }

        @Override
        public Integer next() {
            int save = curr;
            seekToNextValid();
            return save;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
