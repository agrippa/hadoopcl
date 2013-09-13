package org.apache.hadoop.io;

public class HadoopCLResizableIntArray implements HadoopCLResizableArray {
    private int[] buffer;
    private int size;

    public HadoopCLResizableIntArray() {
        buffer = new int[512];
        size = 0;
    }

    public HadoopCLResizableIntArray(int initLength) {
        buffer = new int[initLength];
        size = 0;
    }

    public void reset() { size = 0; }

    public void addAll(int[] other, int N) {
        int newBufferLength = this.buffer.length;
        while(newBufferLength < N) { newBufferLength *= 2; }
        if(newBufferLength > buffer.length) {
            buffer = new int[newBufferLength];
        }

        System.arraycopy(other, 0, this.buffer, 0, N);
        this.size = N;
    }

    public void add(int val) {
        if(size == buffer.length) {
            int[] tmp = new int[buffer.length * 2];
            System.arraycopy(buffer, 0, tmp, 0, buffer.length);
            buffer = tmp;
        }
        buffer[size] = val;
        size = size + 1;
    }

    public void set(int index, int val) {
        ensureCapacity(index+1);
        unsafeSet(index, val);
    }

    public void unsafeSet(int index, int val) {
        buffer[index] = val;
        size = (index + 1 > size ? index + 1 : size);
    }

    public int get(int index) {
        return this.buffer[index];
    }

    public Object getArray() { return buffer; }
    public int size() { return size; }
    public int length() { return buffer.length; }

    public void copyTo(HadoopCLResizableArray other) {
        HadoopCLResizableIntArray actual = (HadoopCLResizableIntArray)other;
        actual.addAll(this.buffer, size);
    }

    public void ensureCapacity(int size) {
        if(buffer.length < size) {
            int n = buffer.length * 2;
            while(n < size) {
                n = n * 2;
            }
            int[] tmp = new int[n];
            System.arraycopy(buffer, 0, tmp, 0, buffer.length);
            buffer = tmp;
        }
    }
}
