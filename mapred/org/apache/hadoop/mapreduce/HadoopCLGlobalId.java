package org.apache.hadoop.mapreduce;

public class HadoopCLGlobalId {
  private final int globalId;
  private int attempt;

  public HadoopCLGlobalId(int globalId) {
    this.globalId = globalId;
    this.attempt = 0;
  }

  private HadoopCLGlobalId(int globalId, int attempt) {
    this.globalId = globalId;
    this.attempt = attempt;
  }

  public void incrementAttempt() {
    this.attempt = this.attempt + 1;
  }

  public int attempt() {
    return this.attempt;
  }

  public int globalId() {
    return this.globalId;
  }

  public HadoopCLGlobalId clone() {
    return new HadoopCLGlobalId(this.globalId, this.attempt);
  }

  @Override
  public String toString() {
    return "["+globalId+","+attempt+"]";
  }
}
