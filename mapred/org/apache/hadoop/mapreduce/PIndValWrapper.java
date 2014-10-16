package org.apache.hadoop.mapreduce;

public class PIndValWrapper implements Comparable<PIndValWrapper> {
    public final double prob;
    public final int[] indices;
    public final double[] dvals;
    public final float[] fvals;
    public final int length;

    public PIndValWrapper(double prob, int[] indices, double[] vals, int length) {
        this.prob = prob;
        this.length = length;
        this.indices = new int[this.length];
        this.dvals = new double[this.length];
        this.fvals = null;
        System.arraycopy(indices, 0, this.indices, 0, this.length);
        System.arraycopy(vals, 0, this.dvals, 0, this.length);
    }

    public PIndValWrapper(double prob, int[] indices, float[] vals, int length) {
        this.prob = prob;
        this.length = length;
        this.indices = new int[this.length];
        this.fvals = new float[this.length];
        this.dvals = null;
        System.arraycopy(indices, 0, this.indices, 0, this.length);
        System.arraycopy(vals, 0, this.fvals, 0, this.length);
    }

    public PIndValWrapper(double prob, int[] indices, int length) {
        this.prob = prob;
        this.length = length;
        this.indices = new int[this.length];
        this.dvals = null;
        this.fvals = null;
        System.arraycopy(indices, 0, this.indices, 0, this.length);
    }

    @Override
    public int compareTo(PIndValWrapper other) {
        return this.length - other.length;
    }

    @Override
    public int hashCode() {
        return this.length;
    }
}
