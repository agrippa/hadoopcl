package org.apache.hadoop.io;

public interface HadoopCLResizableArray {
    public void reset();
    public int size();
    public int length();
    public void copyTo(HadoopCLResizableArray other);
    public Object getArray();
    public void ensureCapacity(int size);
    public long space();
}
