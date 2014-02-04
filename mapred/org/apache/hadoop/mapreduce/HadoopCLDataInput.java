package org.apache.hadoop.mapreduce;

import java.io.DataInput;

public interface HadoopCLDataInput extends DataInput {
    public boolean hasMore();
    public void nextKey();
    public void nextValue();
    public void prev();

    public void readFully(int[] b, int offset, int len);
    public void readFully(double[] b, int offset, int len);
    // public int currentKeySize();
    // public int currentValueSize();
}
