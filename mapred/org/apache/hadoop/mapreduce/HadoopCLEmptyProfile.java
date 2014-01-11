package org.apache.hadoop.mapreduce;

import java.util.List;

public class HadoopCLEmptyProfile implements HadoopCLProfile {
  private final HadoopOpenCLContext clContext;
  public HadoopCLEmptyProfile(HadoopOpenCLContext clContext) {
    this.clContext = clContext;
  }

  public void addItemsProcessed(int count) { }
  public void startRead(HadoopCLBuffer owner) {
    // LOG:PROFILE
    // OpenCLDriver.logger.log("starting read of "+owner.tracker.toString(), this.clContext);
  }
  public void stopRead(HadoopCLBuffer owner) {
    // LOG:PROFILE
    // OpenCLDriver.logger.log("finishing read of "+owner.tracker.toString(), this.clContext);
  }
  public void startKernel() { }
  public void stopKernel() { }
  public void startWrite(HadoopCLBuffer owner) { 
    // LOG:PROFILE
    // OpenCLDriver.logger.log("starting write of "+owner.tracker.toString(), this.clContext);
  }
  public void stopWrite(HadoopCLBuffer owner) {
    // LOG:PROFILE
    // OpenCLDriver.logger.log("finishing write of "+owner.tracker.toString(), this.clContext);
  }
  public long readTime() { return 0L; }
  public long kernelTime() { return 0L; }
  public long writeTime() { return 0L; }
  public int nItemsProcessed() { return 0; }
  public int nKernelAttempts() { return 0; }
  public String listToString(List<HadoopCLProfile> profiles) { return ""; }
}
