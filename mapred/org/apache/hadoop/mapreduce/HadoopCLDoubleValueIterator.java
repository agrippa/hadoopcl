package org.apache.hadoop.mapreduce;

import org.apache.hadoop.io.HadoopCLResizableDoubleArray;

public class HadoopCLDoubleValueIterator {
    private double[] vals;
    private int len;
    private int currentIndex;

    public HadoopCLDoubleValueIterator(double[] setVals, int setLen) {
        this.vals = setVals; this.len = setLen;
        this.currentIndex = 0;
    }

    public boolean next() {
        if (this.currentIndex == this.len-1) return false;
        this.currentIndex = this.currentIndex + 1;
        return true;
    }

    public boolean seekTo(int set) {
        if (set >= this.len) return false;
        this.currentIndex = set;
        return true;
    }

    public int current() {
        return this.currentIndex;
    }

    public int nValues() {
        return this.len;
    }

    public double get() {
        return this.vals[this.currentIndex];
    }

}
