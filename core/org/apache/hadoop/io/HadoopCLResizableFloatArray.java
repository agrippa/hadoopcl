package org.apache.hadoop.io;

public class HadoopCLResizableFloatArray implements HadoopCLResizableArray {
    private float[] buffer;
    private int size;

    public HadoopCLResizableFloatArray() {
        buffer = new float[512];
        size = 0;
    }

    public HadoopCLResizableFloatArray(int initLength) {
        buffer = new float[initLength];
        size = 0;
    }

    public void reset() { size = 0; }
    public void forceSize(int s) { this.size = s; }

    public void addAll(float[] other, int N) {
        int newBufferLength = this.buffer.length;
        while(newBufferLength < N) { newBufferLength *= 2; }
        if(newBufferLength > buffer.length) {
            buffer = new float[newBufferLength];
        }

        System.arraycopy(other, 0, this.buffer, 0, N);
        this.size = N;
    }

    public void add(float val) {
        if(size == buffer.length) {
            float[] tmp = new float[buffer.length * 2];
            System.arraycopy(buffer, 0, tmp, 0, buffer.length);
            buffer = tmp;
        }
        buffer[size] = val;
        size = size + 1;
    }

    public void set(int index, float val) {
        ensureCapacity(index+1);
        unsafeSet(index, val);
    }

    public void unsafeSet(int index, float val) {
        buffer[index] = val;
        size = (index + 1 > size ? index + 1 : size);
    }

    public float get(int index) {
        return this.buffer[index];
    }

    public Object getArray() { return buffer; }
    public int size() { return size; }
    public int length() { return buffer.length; }

    public void copyTo(HadoopCLResizableArray other) {
        HadoopCLResizableFloatArray actual = (HadoopCLResizableFloatArray)other;
        actual.addAll(this.buffer, size);
    }

    public void ensureCapacity(int size) {
        if(buffer.length < size) {
            int n = (int)(buffer.length * 1.3);
            n = (n > size ? n : size);
            float[] tmp = new float[n];
            System.arraycopy(buffer, 0, tmp, 0, buffer.length);
            buffer = tmp;
        }
    }

    public long space() {
        return this.buffer.length * 4;
    }

}
