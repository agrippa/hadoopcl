package org.apache.hadoop.io;

public class HadoopCLResizableDoubleArray implements HadoopCLResizableArray {
    private double[] buffer;
    private int size;

    public HadoopCLResizableDoubleArray() {
        buffer = new double[512];
        size = 0;
    }

    public HadoopCLResizableDoubleArray(int initLength) {
        buffer = new double[initLength];
        size = 0;
    }

    public void reset() { size = 0; }
    public void forceSize(int s) { this.size = s; }

    public void addAll(double[] other, int N) {
        int newBufferLength = this.buffer.length;
        while(newBufferLength < N) { newBufferLength *= 2; }
        if(newBufferLength > buffer.length) {
            buffer = new double[newBufferLength];
        }

        System.arraycopy(other, 0, this.buffer, 0, N);
        this.size = N;
    }

    public void add(double val) {
        if(size == buffer.length) {
            double[] tmp = new double[buffer.length * 2];
            System.arraycopy(buffer, 0, tmp, 0, buffer.length);
            buffer = tmp;
        }
        buffer[size] = val;
        size = size + 1;
    }

    public void set(int index, double val) {
        ensureCapacity(index+1);
        unsafeSet(index, val);
    }

    public void unsafeSet(int index, double val) {
        buffer[index] = val;
        size = (index + 1 > size ? index + 1 : size);
    }

    public double get(int index) {
        return this.buffer[index];
    }

    public Object getArray() { return buffer; }
    public int size() { return size; }
    public int length() { return buffer.length; }

    public void copyTo(HadoopCLResizableArray other) {
        HadoopCLResizableDoubleArray actual = (HadoopCLResizableDoubleArray)other;
        actual.addAll(this.buffer, size);
    }

    public void ensureCapacity(int size) {
        if(buffer.length < size) {
            int n = (int)(buffer.length * 1.3);
            n = (n > size ? n : size);
            double[] tmp = new double[n];
            System.arraycopy(buffer, 0, tmp, 0, buffer.length);
            buffer = tmp;
        }
    }

    public long space() {
        return this.buffer.length * 8;
    }

}
