package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeSet;
import org.apache.hadoop.mapred.RawKeyValueIterator;

public abstract class HadoopCLKeyValueIterator implements RawKeyValueIterator {
    public TreeSet<Integer> sortedIndices;
}
