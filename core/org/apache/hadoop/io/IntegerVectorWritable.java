package org.apache.hadoop.io;

import java.io.*;

public class IntegerVectorWritable implements WritableComparable<IntegerVectorWritable> {
    private int[] vals;
    private int overrideLength;
    private HadoopCLResizableIntArray valsRes;

    public IntegerVectorWritable() {
        this.vals = null;
        this.valsRes = null;
        this.overrideLength = -1;
    }

    public IntegerVectorWritable(int[] vals) {
        this.vals = vals;
        this.valsRes = null;
        this.overrideLength = -1;
    }

    public IntegerVectorWritable(int[] vals, int offset, int length) {
        this.vals = new int[length];
        System.arraycopy(vals, offset, this.vals, 0, length);
        this.valsRes = null;
        this.overrideLength = -1;
    }

    public IntegerVectorWritable(HadoopCLResizableIntArray vals) {
        this.valsRes = vals;
        this.vals = null;
        this.overrideLength = -1;
    }

    public void set(int[] vals) {
        this.vals = vals;
        this.valsRes = null;
        this.overrideLength = -1;
    }

    public void set(int[] vals, int len) {
        this.vals = vals;
        this.valsRes = null;
        this.overrideLength = len;
    }

    public void set(HadoopCLResizableIntArray setVals) {
        this.valsRes = setVals;
        this.vals = null;
        this.overrideLength = -1;
    }

    public int[] vals() {
        return this.vals == null ? (int[])this.valsRes.getArray() : this.vals;
    }
    
    public int size() {
        if(this.overrideLength != -1) {
            return this.overrideLength;
        } else {
            return this.vals == null ? this.valsRes.size() : this.vals.length;
        }
    }

    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        this.vals = new int[len];
        this.valsRes = null;
        this.overrideLength = -1;

        for (int i = 0; i < len; i++) {
            this.vals[i] = in.readInt();
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.size());
        if(this.vals != null) {
            for(int i = 0; i < this.size(); i++) {
                out.writeInt(this.vals[i]);
            }
        } else {
            int[] privateVals = (int[])this.valsRes.getArray();

            for(int i = 0; i < this.size(); i++) {
                out.writeInt(privateVals[i]);
            }
        }
    }

    private boolean elementEqual(IntegerVectorWritable other, int index) {
        int thisIndex = this.vals()[index];
        int otherIndex = other.vals()[index];
        return otherIndex == thisIndex;
    }

    public boolean equals(Object o) {
        if(o instanceof IntegerVectorWritable) {
            IntegerVectorWritable other = (IntegerVectorWritable)o;
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
        int sum = 0;
        for(int i = 0; i < this.size(); i++) {
            sum = sum + (this.vals()[i] * this.vals()[i]);
        }
        return Math.sqrt(sum);
    }

    public int compareTo(IntegerVectorWritable other) {
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

    public IntegerVectorWritable clone() {
        IntegerVectorWritable c = new IntegerVectorWritable();
        c.vals = new int[this.size()];
        System.arraycopy(this.vals, 0, c.vals, 0, this.size());
        return c;
    }

    public String toString(int n) {
        StringBuffer str = new StringBuffer();
        str.append("{ ");
        int length = (this.size() < n ? this.size() : n);
        for(int i = 0; i < length; i++) {
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
            str.append(this.vals()[i]);
            str.append(" ");
        }
        str.append("}");
        return str.toString();
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntegerVectorWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            int length1 = readInt(b1, s1);
            int length2 = readInt(b2, s2);
            s1 += 4; // increment for length reads
            s2 += 4;

            int sum1 = 0;
            int sum2 = 0;
            for(int i = 0; i < length1; i++) {
                int curr = readInt(b1, s1);
                s1 += 4;
                sum1 = sum1 + (curr * curr);
            }
            for(int i = 0; i < length2; i++) {
                int curr = readInt(b2, s2);
                s2 += 4;
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
        WritableComparator.define(IntegerVectorWritable.class, new Comparator());
    }
}
