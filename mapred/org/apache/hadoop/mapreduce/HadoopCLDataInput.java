package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.io.DataInput;

public interface HadoopCLDataInput extends DataInput {
    public boolean hasMore() throws IOException;
    public void nextKey() throws IOException;
    public void nextValue() throws IOException;
    public void prev() throws IOException;
    public void reset();
    public int compareKeys(HadoopCLDataInput other) throws IOException;

    public void readFully(int[] b, int offset, int len) throws IOException;
    public void readFully(double[] b, int offset, int len) throws IOException;
    public void readFully(float[] b, int offset, int len) throws IOException;
}
