package org.apache.hadoop.mapreduce;

public class HadoopCLKernelContext {
    final HadoopCLInputBuffer inputBuffer;
    final HadoopCLOutputBuffer outputBuffer;
    final HadoopCLKernel kernel;

    public HadoopCLKernelContext(HadoopCLKernel kernel,
            HadoopCLInputBuffer inputBuffer, HadoopCLOutputBuffer outputBuffer) {
        this.kernel = kernel;
        this.inputBuffer = inputBuffer;
        this.outputBuffer = outputBuffer;
    }

    public HadoopCLKernel kernel() { return this.kernel; }
    public HadoopCLInputBuffer inputBuffer() { return this.inputBuffer; }
    public HadoopCLOutputBuffer outputBuffer() { return this.outputBuffer; }
}
