package org.apache.hadoop.mapreduce;

public class HadoopCLMutableInteger {
    private int val;
    public HadoopCLMutableInteger() {
        this.val = 0;
    }

    public void incr() {
        this.val++;
    }

    public void decr() {
        this.val--;
    }

    public int get() {
        return this.val;
    }
}
