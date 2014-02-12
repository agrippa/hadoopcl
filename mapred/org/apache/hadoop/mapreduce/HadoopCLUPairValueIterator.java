package org.apache.hadoop.mapreduce;



public class HadoopCLUPairValueIterator {
    private int[] valIds;
    private double[] vals1;
    private double[] vals2;
    int len;
    private int currentIndex;

    public HadoopCLUPairValueIterator(int[] setValIds, double[] setVals1, double[] setVals2, int setLen) {
        this.valIds = setValIds; this.vals1 = setVals1; this.vals2 = setVals2; this.len = setLen;
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

    public int getValId() {
        return this.valIds[this.currentIndex];
    }

    public double getVal1() {
        return this.vals1[this.currentIndex];
    }

    public double getVal2() {
        return this.vals2[this.currentIndex];
    }

}
