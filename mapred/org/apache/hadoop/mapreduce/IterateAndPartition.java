package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapred.RawKeyValueIterator;

public interface IterateAndPartition extends RawKeyValueIterator, HadoopCLPartitioner {
}
