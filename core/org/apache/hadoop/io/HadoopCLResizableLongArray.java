package org.apache.hadoop.io;

public class HadoopCLResizableLongArray implements HadoopCLResizableArray {
    private long[] buffer;
    private int size;

    public HadoopCLResizableLongArray() {
        buffer = new long[512];
        size = 0;
    }

    public HadoopCLResizableLongArray(int initLength) {
        buffer = new long[initLength];
        size = 0;
    }

    public void reset() { size = 0; }

    public void addAll(long[] other, int N) {
        int newBufferLength = this.buffer.length;
        while(newBufferLength < N) { newBufferLength *= 2; }
        if(newBufferLength > buffer.length) {
            buffer = new long[newBufferLength];
        }

        System.arraycopy(other, 0, this.buffer, 0, N);
        this.size = N;
    }

    public void add(long val) {
        if(size == buffer.length) {
            long[] tmp = new long[buffer.length * 2];
            System.arraycopy(buffer, 0, tmp, 0, buffer.length);
            buffer = tmp;
        }
        buffer[size] = val;
        size = size + 1;
    }

    public void set(int index, long val) {
        ensureCapacity(index+1);
        unsafeSet(index, val);
    }

    public void unsafeSet(int index, long val) {
        buffer[index] = val;
        size = (index + 1 > size ? index + 1 : size);
    }

    public long get(int index) {
        return this.buffer[index];
    }

    public Object getArray() { return buffer; }
    public int size() { return size; }
    public int length() { return buffer.length; }

    public void copyTo(HadoopCLResizableArray other) {
        HadoopCLResizableLongArray actual = (HadoopCLResizableLongArray)other;
        actual.addAll(this.buffer, size);
    }

    public void ensureCapacity(int size) {
        if(buffer.length < size) {
            int n = buffer.length * 2;
            while(n < size) {
                n = n * 2;
            }
            long[] tmp = new long[n];
            System.arraycopy(buffer, 0, tmp, 0, buffer.length);
            buffer = tmp;
        }
    }
}
