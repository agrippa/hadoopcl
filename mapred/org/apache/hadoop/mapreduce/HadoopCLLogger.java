package org.apache.hadoop.mapreduce;

public class HadoopCLLogger {
  private final boolean enabled;

  public HadoopCLLogger(boolean enabled) {
    this.enabled = enabled;
  }

  public void log(String msg, HadoopOpenCLContext ctx) {
    this.log(msg, ctx.verboseTypeName());
  }

  public void log(String msg, String ctxType) {
    if (this.enabled) {
      System.out.println("TIMING | "+ctxType+" | "+System.currentTimeMillis()+" | "+msg);
    }
  }
}
