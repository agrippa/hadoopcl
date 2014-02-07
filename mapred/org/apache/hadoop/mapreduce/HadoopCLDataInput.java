package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.io.DataInput;

public interface HadoopCLDataInput extends DataInput {
    public boolean hasMore();
    public void nextKey() throws IOException;
    public void nextValue() throws IOException;
    public void prev();

    public void readFully(int[] b, int offset, int len);
    public void readFully(double[] b, int offset, int len);
    public void readFully(float[] b, int offset, int len);
}
