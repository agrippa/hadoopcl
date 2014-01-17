package org.apache.hadoop.mapreduce;

import java.io.DataInput;

public interface HadoopCLDataInput extends DataInput {
    public boolean hasMore();
}
