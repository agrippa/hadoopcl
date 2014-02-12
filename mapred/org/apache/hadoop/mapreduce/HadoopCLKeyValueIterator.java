package org.apache.hadoop.mapreduce;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.Progress;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeSet;
import org.apache.hadoop.mapred.RawKeyValueIterator;

public abstract class HadoopCLKeyValueIterator implements RawKeyValueIterator {
    public TreeSet<IntegerPair> sortedIndices;
    protected IntegerPair current = null;
    protected ByteBuffer keyBytes = null;
    protected ByteBuffer valueBytes = null;
    protected final DataInputBuffer key = new DataInputBuffer();
    protected final DataInputBuffer value = new DataInputBuffer();

    public static class IntegerPair {
        public final int buffer;
        public final int index;
        public IntegerPair(int buffer, int index) {
            this.buffer = buffer;
            this.index = index;
        }
    }

    protected ByteBuffer resizeByteBuffer(ByteBuffer buf, int len) {
        if (buf == null || buf.capacity() < len) {
            return ByteBuffer.allocate(len);
        } else {
            return buf;
        }
    }

    @Override
    public final boolean next() throws IOException {
        if (sortedIndices.isEmpty()) {
            return false;
        } else {
            this.current = sortedIndices.pollFirst();
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
}
