package org.apache.hadoop.mapreduce;

public class HadoopCLAccumulatedProfile implements IHadoopCLAccumulatedProfile {
    private long accumRead = 0;
    private long accumKernel = 0;
    private long accumWrite = 0;
    private long accumKeys = 0;
    private long overallTime = 0;

    private long startRead = -1;
    private long startKernel = -1;
    private long startWrite = -1;
    private long overallStart = -1;

    public void startOverall() {
        this.overallStart = System.currentTimeMillis();
        this.startRead = this.startKernel = this.startWrite = -1;
        this.accumRead = this.accumKernel = this.accumWrite = 0;
        this.overallTime = 0;
    }

    public void startRead() {
        if (this.startRead != -1) {
            throw new RuntimeException("Overwriting last read start time");
        }
        this.startRead = System.currentTimeMillis();
        this.accumKeys++;
    }

    public void startKernel() {
        if (this.startKernel != -1) {
            throw new RuntimeException("Overwriting last kernel start time");
        }
        this.startKernel = System.currentTimeMillis();
    }

    public void startWrite() {
        if (this.startWrite != -1) {
            throw new RuntimeException("Overwriting last write start time");
        }
        this.startWrite = System.currentTimeMillis();
    }

    public void stopOverall() {
        this.overallTime = (System.currentTimeMillis() - this.overallStart);
    }

    public void stopRead() {
        if (this.startRead == -1) {
            throw new RuntimeException("Stopping read that was never started");
        }
        this.accumRead += (System.currentTimeMillis() - this.startRead);
        this.startRead = -1;
    }

    public void stopWrite() {
        if (this.startWrite == -1) {
            throw new RuntimeException("Stopping write that was never started");
        }
        this.accumWrite += (System.currentTimeMillis() - this.startWrite);
        this.startWrite = -1;
    }

    public void stopKernel() {
        if (this.startKernel == -1) {
            throw new RuntimeException("Stopping kernel that was never started");
        }
        this.accumKernel += (System.currentTimeMillis() - this.startKernel);
        this.startKernel = -1;
    }

    public long totalReadTime() {
        return this.accumRead;
    }
    public long totalKernelTime() {
        return this.accumKernel;
    }
    public long totalWriteTime() {
        return this.accumWrite;
    }
    public long totalKeysProcessed() {
      return this.accumKeys;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("runTime = ");
        sb.append(this.overallTime);
        sb.append(" ms, readTime = ");
        sb.append(this.accumRead);
        sb.append(" ms, kernelTime = ");
        sb.append(this.accumKernel);
        sb.append(" ms, writeTime = ");
        sb.append(this.accumWrite);
        sb.append(" ms, keysProcessed = ");
        sb.append(this.accumKeys);
        return sb.toString();
    }
}
