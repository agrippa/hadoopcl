package org.apache.hadoop.mapreduce;

public class IndValWrapper implements Comparable<IndValWrapper> {
    public final int[] indices;
    public final double[] dvals;
    public final float[] fvals;
    public final int length;

    public IndValWrapper(int[] indices, double[] vals, int length) {
        this.length = length;
        this.indices = new int[this.length];
        this.dvals = new double[this.length];
        this.fvals = null;
        System.arraycopy(indices, 0, this.indices, 0, this.length);
        System.arraycopy(vals, 0, this.dvals, 0, this.length);
    }

    public IndValWrapper(int[] indices, float[] vals, int length) {
        this.length = length;
        this.indices = new int[this.length];
        this.fvals = new float[this.length];
        this.dvals = null;
        System.arraycopy(indices, 0, this.indices, 0, this.length);
        System.arraycopy(vals, 0, this.fvals, 0, this.length);
    }

    public IndValWrapper(int[] indices, int length) {
        this.length = length;
        this.indices = new int[this.length];
        this.dvals = null;
        this.fvals = null;
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
