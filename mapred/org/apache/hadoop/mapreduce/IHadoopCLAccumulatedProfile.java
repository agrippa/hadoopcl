package org.apache.hadoop.mapreduce;

public interface IHadoopCLAccumulatedProfile {
  public void startOverall();
  public void startRead();
  public void startKernel();
  public void startWrite();
  public void stopOverall();
  public void stopRead();
  public void stopWrite();
  public void stopKernel();
  public long totalReadTime();
  public long totalKernelTime();
  public long totalWriteTime();
  public long totalKeysProcessed();
  public long totalValsProcessed();
  public void addVals(int vals);
}
