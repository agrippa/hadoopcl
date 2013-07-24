
package org.apache.hadoop.io;

import java.io.*;

/** A WritableComparable for ints. */
public class PairWritable implements WritableComparable {
  private double val1;
  private double val2;

  public PairWritable() {}

  public PairWritable(double val1, double val2) { set(val1, val2); }

  /** Set the value of this IntWritable. */
  public void set(double val1, double val2) { 
      this.val1 = val1;
      this.val2 = val2;
  }

  /** Return the value of this IntWritable. */
  public double getVal1() {
      return val1;
  }
  public double getVal2() {
      return val2;
  }

  public void readFields(DataInput in) throws IOException {
    val1 = in.readDouble();
    val2 = in.readDouble();
  }

  public void write(DataOutput out) throws IOException {
    out.writeDouble(val1);
    out.writeDouble(val2);
  }

  /** Returns true iff <code>o</code> is a IntWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof PairWritable))
      return false;
    PairWritable other = (PairWritable)o;
    return this.val1 == other.val1 && this.val2 == other.val2;
  }

  public int hashCode() {
    return (int)val1 + (int)val2;
  }

  /** Compares two IntWritables. */
  public int compareTo(Object o) {
    double thisVal1 = this.val1;
    double thatVal1 = ((PairWritable)o).val1;
    if(thisVal1 < thatVal1) {
        return -1;
    } else if (thisVal1 > thatVal1) {
        return 1;
    } else {
        double thisVal2 = this.val2;
        double thatVal2 = ((PairWritable)o).val2;
        // equal
        if(thisVal2 < thatVal2) {
            return -1;
        } else if(thisVal2 > thatVal2) {
            return 1;
        } else {
            return 0;
        }
    }
  }

  public String toString() {
    return "("+Double.toString(val1)+","+Double.toString(val2)+")";
  }

  /** A Comparator optimized for IntWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(PairWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      double thisVal1 = readDouble(b1, s1);
      double thatVal1 = readDouble(b2, s2);
      if(thisVal1 < thatVal1) {
          return -1;
      } else if (thisVal1 > thatVal1) {
          return 1;
      } else {
          double thisVal2 = readDouble(b1, s1 + 8);
          double thatVal2 = readDouble(b2, s2 + 8);
          // equal
          if(thisVal2 < thatVal2) {
              return -1;
          } else if(thisVal2 > thatVal2) {
              return 1;
          } else {
              return 0;
          }
      }
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(PairWritable.class, new Comparator());
  }
}

