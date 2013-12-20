package org.apache.hadoop.mapreduce;

import java.util.List;

public interface HadoopCLProfile {
  public void addItemProcessed();
  public void startRead(HadoopCLBuffer owner);
  public void stopRead(HadoopCLBuffer owner);
  public void startKernel();
  public void stopKernel();
  public void startWrite(HadoopCLBuffer owner);
  public void stopWrite(HadoopCLBuffer owner);
  public long readTime();
  public long kernelTime();
  public long writeTime();
  public int nItemsProcessed();
  public int nKernelAttempts();
  public String listToString(List<HadoopCLProfile> profiles);
}
