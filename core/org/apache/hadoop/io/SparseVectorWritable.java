package org.apache.hadoop.io;

import java.io.*;

public class SparseVectorWritable implements WritableComparable {
    private int[] indices;
    private double[] vals;
    private HadoopCLResizableIntArray indicesRes;
    private HadoopCLResizableDoubleArray valsRes;

    public SparseVectorWritable() {
        this.indices = null;
        this.vals = null;
    }

    public SparseVectorWritable(int[] indices, double[] vals) {
        this.indices = indices;
        this.vals = vals;
        this.indicesRes = null;
        this.valsRes = null;
    }

    public SparseVectorWritable(HadoopCLResizableIntArray indices, HadoopCLResizableDoubleArray vals) {
        this.indicesRes = indices;
        this.valsRes = vals;
        this.indices = null;
        this.vals = null;
    }

    public void set(int[] indices, double[] vals) {
        this.indices = indices;
        this.vals = vals;
        this.indicesRes = null;
        this.valsRes = null;
    }

    public void set(HadoopCLResizableIntArray setIndices, 
            HadoopCLResizableDoubleArray setVals) {
        this.indicesRes = setIndices;
        this.valsRes = setVals;
        this.indices = null;
        this.vals = null;
    }

    public int[] indices() {
        return this.indices == null ? (int[])this.indicesRes.getArray() : this.indices;
    }
    
    public double[] vals() {
        return this.vals == null ? (double[])this.valsRes.getArray() : this.vals;
    }

    public int size() {
        return this.indices == null ? this.indicesRes.size() : this.indices.length;
    }

    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        this.indices = new int[len];
        this.vals = new double[len];

        for (int i = 0; i < len; i++) {
            this.indices[i] = in.readInt();
            this.vals[i] = in.readDouble();
        }
    }

    public void write(DataOutput out) throws IOException {
        if(this.indices != null) {
            out.writeInt(this.indices.length);
            for(int i = 0; i < this.indices.length; i++) {
                out.writeInt(this.indices[i]);
                out.writeDouble(this.vals[i]);
            }
        } else {
            out.writeInt(this.indicesRes.size());
            int[] privateIndices = (int[])this.indicesRes.getArray();
            double[] privateVals = (double[])this.valsRes.getArray();

            for(int i = 0; i < this.indicesRes.size(); i++) {
                out.writeInt(privateIndices[i]);
                out.writeDouble(privateVals[i]);
            }
        }
    }

    private boolean elementEqual(SparseVectorWritable other, int index) {

        int thisIndex = this.indices()[index];
        double thisVal = this.vals()[index];

        int otherIndex = other.indices()[index];
        double otherVal = other.vals()[index];

        return otherIndex == thisIndex && otherVal == thisVal;
    }

    public boolean equals(Object o) {
        if(o instanceof SparseVectorWritable) {
            SparseVectorWritable other = (SparseVectorWritable)o;
            if(this.size() == other.size()) {
                for(int i = 0; i < this.size(); i++) {
                    if(!this.elementEqual(other, i)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return this.size();
    }

    private double distFromOrigin() {
        double sum = 0.0;
        for(int i = 0; i < this.size(); i++) {
            sum = sum + (this.vals()[i] * this.vals()[i]);
        }
        return Math.sqrt(sum);
    }

    public int compareTo(Object o) {
        SparseVectorWritable other = (SparseVectorWritable)o;
        double thisDist = this.distFromOrigin();
        double otherDist = other.distFromOrigin();
        if(thisDist < otherDist) {
            return -1;
        } else if (thisDist > otherDist) {
            return 1;
        } else {
            return 0;
        }
    }

    public String toString(int n) {
        StringBuffer str = new StringBuffer();
        str.append("{ ");
        int length = (this.size() < n ? this.size() : n);
        for(int i = 0; i < length; i++) {
            str.append(this.indices()[i]);
            str.append(":");
            str.append(this.vals()[i]);
            str.append(" ");
        }
        if(length < this.size()) {
            str.append("... ");
        }
        str.append("}");
        return str.toString();
    }

    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("{ ");
        for(int i = 0; i < this.size(); i++) {
            str.append(this.indices()[i]);
            str.append(":");
            str.append(this.vals()[i]);
            str.append(" ");
        }
        str.append("}");
        return str.toString();
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(SparseVectorWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            int length1 = readInt(b1, s1);
            int length2 = readInt(b2, s2);
            s1 += 4; // increment for length reads
            s2 += 4;

            double sum1 = 0.0;
            double sum2 = 0.0;
            for(int i = 0; i < length1; i++) {
                s1 += 4; // don't really need the index
                double curr = readDouble(b1, s1);
                s1 += 8;
                sum1 = sum1 + (curr * curr);
            }
            for(int i = 0; i < length2; i++) {
                s2 += 4;
                double curr = readDouble(b2, s2);
                s2 += 8;
                sum2 = sum2 + (curr * curr);
            }

            if(sum1 > sum2) {
                return 1;
            } else if(sum1 < sum2) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    static {
        WritableComparator.define(SparseVectorWritable.class, new Comparator());
    }
}
