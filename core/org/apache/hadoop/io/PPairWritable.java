
package org.apache.hadoop.io;

import java.io.*;

/** A WritableComparable for ints. */
public class PPairWritable implements WritableComparable {
  private int id;
  private double val;

  public PPairWritable() {}

  public PPairWritable(int id, double val) { set(id, val); }

  /** Set the value of this IntWritable. */
  public void set(int id, double val) { 
      this.id = id;
      this.val = val;
  }

  /** Return the value of this IntWritable. */
  public int getId() { return this.id; }
  public double getVal() { return val; }

  public void readFields(DataInput in) throws IOException {
    id = in.readInt();
    val = in.readDouble();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
    out.writeDouble(val);
  }

  /** Returns true iff <code>o</code> is a IntWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof PPairWritable))
      return false;
    PPairWritable other = (PPairWritable)o;
    return this.id == other.id;
  }

  public int hashCode() {
      return this.id;
  }

  /** Compares two IntWritables. */
  public int compareTo(Object o) {
    return this.id - ((PPairWritable)o).id;
  }

  public PPairWritable clone() {
      return new PPairWritable(this.id, this.val);
  }

  public String toString() {
    return "("+Integer.toString(id)+","+Double.toString(val)+")";
  }

  /** A Comparator optimized for IntWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(PPairWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int thisId = readInt(b1, s1);
      int thatId = readInt(b2, s2);
      return thisId - thatId;
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(PPairWritable.class, new Comparator());
  }
}

