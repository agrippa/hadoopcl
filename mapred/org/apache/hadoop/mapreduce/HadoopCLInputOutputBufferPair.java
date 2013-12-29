package org.apache.hadoop.mapreduce;

public class HadoopCLInputOutputBufferPair {
    // private final HadoopCLInputBuffer inputBuffer;
    // private final HadoopCLOutputBuffer outputBuffer;
    private final Thread wrapperThread;

    public HadoopCLInputOutputBufferPair( /* HadoopCLInputBuffer inputBuffer,
            HadoopCLOutputBuffer outputBuffer, */ final HadoopCLKernel kernel) {
        // this.inputBuffer = inputBuffer;
        // this.outputBuffer = outputBuffer;
        this.wrapperThread = new Thread(new Runnable() {
          @Override
          public void run() {
            kernel.waitForCompletion();

            synchronized(BufferRunner.somethingHappened) {
                BufferRunner.somethingHappened.set(true);
                BufferRunner.somethingHappened.notify();
            }
          }
        });
        this.wrapperThread.start();
    }

    // public HadoopCLInputBuffer inputBuffer() { return this.inputBuffer; }
    // public HadoopCLOutputBuffer outputBuffer() { return this.outputBuffer; }
    public Thread wrapperThread() { return this.wrapperThread; }
}
