package org.apache.hadoop.mapreduce;

import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.Progress;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeSet;
import org.apache.hadoop.mapred.RawKeyValueIterator;

public abstract class HadoopCLKeyValueIterator implements IterateAndPartition {
    protected final List<ArrayList<IntegerPair>> sortedIndicesList =
        new LinkedList<ArrayList<IntegerPair>>();
    public ArrayList<IntegerPair> sortedIndices;
    int sortedIndicesIter = 0;

    protected IntegerPair current = null;
    protected ByteBuffer keyBytes = null;
    protected ByteBuffer valueBytes = null;
    protected final DataInputBuffer key = new DataInputBuffer();
    protected final DataInputBuffer value = new DataInputBuffer();
    private boolean complete = false;

    protected final int maxAtOnce = 300000;

    public void recalculateLimit() {
        this.complete = false;
        current = null;

        this.sortedIndices = this.sortedIndicesList.remove(0);
        this.sortedIndicesIter = 0;
    }

    public boolean allDone() {
        return sortedIndicesList.isEmpty();
    }

    protected ByteBuffer resizeByteBuffer(ByteBuffer buf, int len) {
        if (buf == null || buf.capacity() < len) {
            return ByteBuffer.allocate(len);
        } else {
            return buf;
        }
    }

    @Override
    public boolean next() throws IOException {
        if (sortedIndicesIter == sortedIndices.size()) {
            return false;
        } else {
            this.current = sortedIndices.get(sortedIndicesIter);
            sortedIndicesIter++;
            return true;
        }
    }

    @Override
    public final void close() throws IOException {
    }

    @Override
    public final Progress getProgress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean supportsBulkReads() {
        return true;
    }

    public static class IntegerPair {
        public final int buffer;
        public final int index;
        public IntegerPair(int buffer, int index) {
            this.buffer = buffer;
            this.index = index;
        }
    }

    public void setComplete() {
        this.complete = true;
    }
    public boolean getComplete() {
        return this.complete;
    }
}
