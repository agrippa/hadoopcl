package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.Deque;
import java.util.Iterator;
import java.util.HashSet;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.io.ReadArrayUtils;

public abstract class HadoopCLOutputBuffer extends HadoopCLBuffer {
    public final int[] memIncr;
    public final int[] memWillRequireRestart;
    protected final int[] outputIterMarkers;
    public HashSet<Integer> itersFinished;
    protected final ReadArrayUtils readUtils = new ReadArrayUtils();

    public HadoopCLOutputBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.memIncr = new int[1];
        this.memWillRequireRestart = new int[1];
        this.outputIterMarkers = new int[this.clContext.getOutputBufferSize()];
    }

    public abstract int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar)
        throws IOException, InterruptedException;

    public abstract Class<?> getOutputKeyClass();
    public abstract Class<?> getOutputValClass();
    public abstract void copyOverFromKernel(HadoopCLKernel kernel);
    public abstract HashSet<Integer> constructIterSet();
    // public abstract HadoopCLKeyValueIterator getKeyValueIterator(List<OutputBufferSoFar> toWrite, int numReduceTasks);
    public abstract int getPartitionFor(int index, int numReduceTasks);
    public abstract int getCount();

    @Override
    public long space() {
        return super.space() + (4 * memIncr.length) +
            (4 * memWillRequireRestart.length);
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
