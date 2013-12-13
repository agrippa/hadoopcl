package org.apache.hadoop.mapreduce;

public class HadoopCLLogger {
  private final boolean enabled;

  public HadoopCLLogger(boolean enabled) {
    this.enabled = enabled;
  }

  public void log(String msg, HadoopOpenCLContext ctx) {
    if (this.enabled) {
      System.err.println(ctx.typeName()+" | "+System.currentTimeMillis()+" | "+msg);
    }
  }
}
