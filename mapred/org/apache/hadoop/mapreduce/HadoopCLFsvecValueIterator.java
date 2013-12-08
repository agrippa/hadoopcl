package org.apache.hadoop.mapreduce;

import org.apache.hadoop.io.HadoopCLResizableIntArray;
import org.apache.hadoop.io.HadoopCLResizableFloatArray;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class HadoopCLFsvecValueIterator {
    private final List<int[]> indices;
    private final List<float[]> vals;
    private int currentIndex;
    private int len;

    public HadoopCLFsvecValueIterator(List<int[]> indices,
                List<float[]> vals) {
        this.indices = indices;
        this.vals = vals;
        this.len = indices.size();
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

    public int[] getValIndices() {
        return this.indices.get(this.currentIndex);
    }

    public float[] getValVals() {
        return this.vals.get(this.currentIndex);
    }

    public int vectorLength(int index) {
        return this.indices.get(index).length;
    }

    public int currentVectorLength() {
        return vectorLength(this.currentIndex);
    }
}
