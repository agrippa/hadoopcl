package org.apache.hadoop.mapreduce;

public class HadoopCLEmptyAccumulatedProfile implements IHadoopCLAccumulatedProfile {
  public void startOverall() { }
  public void startRead() { }
  public void startKernel() { }
  public void startWrite() { }
  public void stopOverall() { }
  public void stopRead() { }
  public void stopWrite() { }
  public void stopKernel() { }
  public long totalReadTime() { return 0L; }
  public long totalKernelTime() { return 0L; }
  public long totalWriteTime() { return 0L; }
  public long totalKeysProcessed() { return 0L; }
  public long totalValsProcessed() { return 0L; }
  public void addVals(int vals) { }
  @Override
  public String toString() { return ""; }
}
