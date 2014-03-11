package org.apache.hadoop.mapreduce;

public interface HadoopCLPartitioner {
    public int getPartitionOfCurrent(int partitions);
}
