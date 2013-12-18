package org.apache.hadoop.mapreduce;

public class DontBlockOnSpillDoneException extends RuntimeException {
  public DontBlockOnSpillDoneException() {
    super("NoBlock");
  }
}
