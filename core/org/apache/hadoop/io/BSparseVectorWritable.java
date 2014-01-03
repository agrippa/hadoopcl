package org.apache.hadoop.io;

import java.nio.*;
import java.io.*;

/*
 * Identical to SparseVectorWritable except for the way it serialized and
 * deserializes itself.
 */
public class BSparseVectorWritable implements WritableComparable {
    private int[] indices;
    private double[] vals;
    private int overrideIndicesOffset;
    private int overrideValsOffset;
    private int overrideLength;
    private HadoopCLResizableIntArray indicesRes;
    private HadoopCLResizableDoubleArray valsRes;

    public BSparseVectorWritable(SparseVectorWritable other) {
      this.indices = new int[other.size()];
      this.vals = new double[other.size()];
      System.arraycopy(other.indices(), 0, this.indices, 0, other.size());
      System.arraycopy(other.vals(), 0, this.vals, 0, other.size());

      this.indicesRes = null;
      this.valsRes = null;
      this.overrideLength = -1;
      this.overrideIndicesOffset = -1;
      this.overrideValsOffset = -1;
    }

    public BSparseVectorWritable() {
        this.indices = null;
        this.vals = null;
        this.indicesRes = null;
        this.valsRes = null;
        this.overrideLength = -1;
        this.overrideIndicesOffset = -1;
        this.overrideValsOffset = -1;
    }

    public BSparseVectorWritable(int[] indices, double[] vals) {
        this.indices = indices;
        this.vals = vals;
        this.indicesRes = null;
        this.valsRes = null;
        this.overrideLength = -1;
        this.overrideIndicesOffset = -1;
        this.overrideValsOffset = -1;
    }

    public BSparseVectorWritable(HadoopCLResizableIntArray indices,
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

    public int indicesOffset() {
        return this.overrideIndicesOffset == -1 ? 0 :
            this.overrideIndicesOffset;
    }

    public int valsOffset() {
        return this.overrideValsOffset == -1 ? 0 : this.overrideValsOffset;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.size());
        ReadArrayUtils.dumpIntArray(out, this.indices(), this.indicesOffset(), this.size());
        ReadArrayUtils.dumpDoubleArray(out, this.vals(), this.valsOffset(), this.size());
    }

    private boolean elementEqual(BSparseVectorWritable other, int index) {

        int thisIndex = this.indices()[this.indicesOffset() + index];
        double thisVal = this.vals()[this.valsOffset() + index];

        int otherIndex = other.indices()[other.indicesOffset() + index];
        double otherVal = other.vals()[other.valsOffset() + index];

        return otherIndex == thisIndex && otherVal == thisVal;
    }

    public boolean equals(Object o) {

        if(o instanceof BSparseVectorWritable) {
            BSparseVectorWritable other = (BSparseVectorWritable)o;
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
        BSparseVectorWritable other = (BSparseVectorWritable)o;
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

    public BSparseVectorWritable clone() {
        BSparseVectorWritable c = new BSparseVectorWritable();
        c.indices = new int[this.size()];
        c.vals = new double[this.size()];
        System.arraycopy(this.indices(), this.indicesOffset(), c.indices, 0, this.size());
        System.arraycopy(this.vals(), this.valsOffset(), c.vals, 0, this.size());
        return c;
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

    public BSparseVectorWritable cloneSparse() {
        int[] newIndices = new int[this.size()];
        double[] newVals = new double[this.size()];

        int indicesOffset = indicesOffset();
        int valsOffset = valsOffset();

        for (int i = 0; i < this.size(); i++) {
            newIndices[i] = this.indices()[indicesOffset + i];
            newVals[i] = this.vals()[valsOffset + i];
        }
        return new BSparseVectorWritable(newIndices, newVals);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(BSparseVectorWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            int length1 = readInt(b1, s1);
            int length2 = readInt(b2, s2);

            int startOfVals1 = s1 + 4 + (length1 * 4);
            int startOfVals2 = s2 + 4 + (length2 * 4);
            double[] vals1 = new double[length1];
            double[] vals2 = new double[length2];

            ByteBuffer.wrap(b1, startOfVals1, length1 * 8).asDoubleBuffer().
              get(vals1);
            ByteBuffer.wrap(b2, startOfVals2, length2 * 8).asDoubleBuffer().
              get(vals2);

            double sum1 = 0.0;
            double sum2 = 0.0;

            for (int i = 0; i < length1; i++) sum1 += (vals1[i] * vals1[i]);
            for (int i = 0; i < length2; i++) sum2 += (vals2[i] * vals2[i]);

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
        WritableComparator.define(BSparseVectorWritable.class,
                new Comparator());
    }
}
