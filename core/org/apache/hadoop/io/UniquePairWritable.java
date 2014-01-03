
package org.apache.hadoop.io;

import java.io.*;

/** A WritableComparable for ints. */
public class UniquePairWritable implements WritableComparable {
  private int ival; 
  private double val1;
  private double val2;

  public UniquePairWritable() { }

  public UniquePairWritable(int ival, double val1, double val2) { 
      set(ival, val1, val2); 
  }

  /** Set the value of this IntWritable. */
  public void set(int id, double val1, double val2) { 
      this.ival = id;
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
  public int getIVal() {
      return ival;
  }

  public void readFields(DataInput in) throws IOException {
    ival = in.readInt();
    val1 = in.readDouble();
    val2 = in.readDouble();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(ival);
    out.writeDouble(val1);
    out.writeDouble(val2);
  }

  /** Returns true iff <code>o</code> is a IntWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof UniquePairWritable))
      return false;
    UniquePairWritable other = (UniquePairWritable)o;
    return this.ival == other.ival && this.val1 == other.val1 && this.val2 == other.val2;
  }

  public int hashCode() {
    return ival;
  }

  /** Compares two IntWritables. */
  public int compareTo(Object o) {
    int thisIval = this.ival;
    double thisVal1 = this.val1;
    double thisVal2 = this.val2;
    int thatIval = ((UniquePairWritable)o).ival;
    double thatVal1 = ((UniquePairWritable)o).val1;
    double thatVal2 = ((UniquePairWritable)o).val2;
    if(thisIval < thatIval) {
        return -1;
    } else if(thisIval > thatIval) {
        return 1;
    } else {
        if(thisVal1 < thatVal1) {
            return -1;
        } else if (thisVal1 > thatVal1) {
            return 1;
        } else {
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

  public UniquePairWritable clone() {
      return new UniquePairWritable(ival, val1, val2);
  }

  public String toString() {
    return "("+Integer.toString(ival)+","+Double.toString(val1)+","+Double.toString(val2)+")";
  }

  /** A Comparator optimized for IntWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(UniquePairWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int thisIval = readInt(b1, s1);
      double thisVal1 = readDouble(b1, s1 + 4);
      double thisVal2 = readDouble(b1, s1 + 4 + 8);
      int thatIval = readInt(b2, s2);
      double thatVal1 = readDouble(b2, s2 + 4);
      double thatVal2 = readDouble(b2, s2 + 4 + 8);
      if(thisIval < thatIval) {
          return -1;
      } else if(thisIval > thatIval) {
          return 1;
      } else {
          if(thisVal1 < thatVal1) {
              return -1;
          } else if (thisVal1 > thatVal1) {
              return 1;
          } else {
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
  }

  static {                                        // register this comparator
    WritableComparator.define(UniquePairWritable.class, new Comparator());
  }
}

