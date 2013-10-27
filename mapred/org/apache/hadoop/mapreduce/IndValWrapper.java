package org.apache.hadoop.mapreduce;

public class IndValWrapper implements Comparable<IndValWrapper> {
    public final int[] indices;
    public final double[] vals;
    public final int length;

    public IndValWrapper(int[] indices, double[] vals, int length) {
        this.length = length;
        this.indices = new int[this.length];
        this.vals = new double[this.length];
        System.arraycopy(indices, 0, this.indices, 0, this.length);
        System.arraycopy(vals, 0, this.vals, 0, this.length);
    }

    public IndValWrapper(int[] indices, int length) {
        this.length = length;
        this.indices = new int[this.length];
        this.vals = null;
        System.arraycopy(indices, 0, this.indices, 0, this.length);
    }

    @Override
    public int compareTo(IndValWrapper other) {
        return this.length - other.length;
    }

    @Override
    public int hashCode() {
        return this.length;
    }
}
