package org.apache.hadoop.io;

import java.io.*;

/*
 * Identical to SparseVectorWritable except for the way it serialized and
 * deserializes itself.
 */
public class NewSparseVectorWritable implements WritableComparable {
    private int[] indices;
    private double[] vals;
    private int overrideIndicesOffset;
    private int overrideValsOffset;
    private int overrideLength;
    private HadoopCLResizableIntArray indicesRes;
    private HadoopCLResizableDoubleArray valsRes;

    public NewSparseVectorWritable() {
        this.indices = null;
        this.vals = null;
        this.indicesRes = null;
        this.valsRes = null;
        this.overrideLength = -1;
        this.overrideIndicesOffset = -1;
        this.overrideValsOffset = -1;
    }

    public NewSparseVectorWritable(int[] indices, double[] vals) {
        this.indices = indices;
        this.vals = vals;
        this.indicesRes = null;
        this.valsRes = null;
        this.overrideLength = -1;
        this.overrideIndicesOffset = -1;
        this.overrideValsOffset = -1;
    }

    public NewSparseVectorWritable(HadoopCLResizableIntArray indices,
            HadoopCLResizableDoubleArray vals) {
        this.indicesRes = indices;
        this.valsRes = vals;
        this.indices = null;
        this.vals = null;
        this.overrideLength = -1;
        this.overrideIndicesOffset = -1;
        this.overrideValsOffset = -1;
    }

    public void set(int[] indices, double[] vals) {
        this.indices = indices;
        this.vals = vals;
        this.indicesRes = null;
        this.valsRes = null;
        this.overrideLength = -1;
        this.overrideIndicesOffset = -1;
        this.overrideValsOffset = -1;
    }

    public void set(int[] indices, double[] vals, int len) {
        this.indices = indices;
        this.vals = vals;
        this.indicesRes = null;
        this.valsRes = null;
        this.overrideLength = len;
        this.overrideIndicesOffset = -1;
        this.overrideValsOffset = -1;
    }

    public void set(HadoopCLResizableIntArray setIndices, 
            HadoopCLResizableDoubleArray setVals) {
        this.indicesRes = setIndices;
        this.valsRes = setVals;
        this.indices = null;
        this.vals = null;
        this.overrideLength = -1;
        this.overrideIndicesOffset = -1;
        this.overrideValsOffset = -1;
    }

    public void set(int[] indices, int indicesOffset,
            double[] vals, int valsOffset, int len) {
        this.indices = indices;
        this.vals = vals;
        this.indicesRes = null;
        this.valsRes = null;
        this.overrideLength = len;
        this.overrideIndicesOffset = indicesOffset;
        this.overrideValsOffset = valsOffset;
    }

    public int[] indices() {
        return this.indices == null ? (int[])this.indicesRes.getArray() :
            this.indices;
    }
    
    public double[] vals() {
        return this.vals == null ? (double[])this.valsRes.getArray() :
            this.vals;
    }

    public int size() {
        if(this.overrideLength != -1) {
            return this.overrideLength;
        } else {
            return this.indices == null ? this.indicesRes.size() :
                this.indices.length;
        }
    }

    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        this.indices = ReadArrayUtils.readIntArray(in, len);
        this.vals = ReadArrayUtils.readDoubleArray(in, len);
        this.indicesRes = null;
        this.valsRes = null;
        this.overrideLength = -1;
        this.overrideIndicesOffset = -1;
        this.overrideValsOffset = -1;
    }

    private int indicesOffset() {
        return this.overrideIndicesOffset == -1 ? 0 :
            this.overrideIndicesOffset;
    }

    private int valsOffset() {
        return this.overrideValsOffset == -1 ? 0 : this.overrideValsOffset;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.size());
        ReadArrayUtils.dumpIntArray(out, this.indices(), this.size());
        ReadArrayUtils.dumpDoubleArray(out, this.vals(), this.size());
    }

    private boolean elementEqual(NewSparseVectorWritable other, int index) {

        int thisIndex = this.indices()[this.indicesOffset() + index];
        double thisVal = this.vals()[this.valsOffset() + index];

        int otherIndex = other.indices()[other.indicesOffset() + index];
        double otherVal = other.vals()[other.valsOffset() + index];

        return otherIndex == thisIndex && otherVal == thisVal;
    }

    public boolean equals(Object o) {

        if(o instanceof NewSparseVectorWritable) {
            NewSparseVectorWritable other = (NewSparseVectorWritable)o;
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
        return this.indices()[this.indicesOffset()];
    }

    private double distFromOrigin() {
        double sum = 0.0;
        int valsOffset = this.valsOffset();
        for(int i = valsOffset; i < valsOffset + this.size(); i++) {
            sum = sum + (this.vals()[i] * this.vals()[i]);
        }
        return Math.sqrt(sum);
    }

    public int compareTo(Object o) {
        NewSparseVectorWritable other = (NewSparseVectorWritable)o;
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
        int indicesOffset = indicesOffset();
        int valsOffset = valsOffset();

        for(int i = 0; i < length; i++) {
            str.append(this.indices()[indicesOffset + i]);
            str.append(":");
            str.append(this.vals()[valsOffset + i]);
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
        int indicesOffset = indicesOffset();
        int valsOffset = valsOffset();

        for(int i = 0; i < this.size(); i++) {
            str.append(this.indices()[indicesOffset + i]);
            str.append(":");
            str.append(this.vals()[valsOffset + i]);
            str.append(" ");
        }
        str.append("}");
        return str.toString();
    }

    public NewSparseVectorWritable cloneSparse() {
        int[] newIndices = new int[this.size()];
        double[] newVals = new double[this.size()];

        int indicesOffset = indicesOffset();
        int valsOffset = valsOffset();

        for (int i = 0; i < this.size(); i++) {
            newIndices[i] = this.indices()[indicesOffset + i];
            newVals[i] = this.vals()[valsOffset + i];
        }
        return new NewSparseVectorWritable(newIndices, newVals);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(NewSparseVectorWritable.class);
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
        WritableComparator.define(NewSparseVectorWritable.class,
                new Comparator());
    }
}
