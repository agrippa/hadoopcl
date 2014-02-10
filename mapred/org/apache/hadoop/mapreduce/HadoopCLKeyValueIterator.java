package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeSet;
import org.apache.hadoop.mapred.RawKeyValueIterator;

public abstract class HadoopCLKeyValueIterator implements RawKeyValueIterator {
    public TreeSet<IntegerPair> sortedIndices;

    public static class IntegerPair {
        public final int buffer;
        public final int index;
        public IntegerPair(int buffer, int index) {
            this.buffer = buffer;
            this.index = index;
        }
    }
}
