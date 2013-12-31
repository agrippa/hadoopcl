package org.apache.hadoop.mapreduce;

public class HadoopCLInputOutputBufferPair {
    // private final HadoopCLInputBuffer inputBuffer;
    // private final HadoopCLOutputBuffer outputBuffer;
    // private final Thread wrapperThread;
/*
    public HadoopCLInputOutputBufferPair(final HadoopCLKernel kernel,
            final ConcurrentLinkedQueue<HadoopCLKernel> toCopyFromOpenCl) {
        // this.inputBuffer = inputBuffer;
        // this.outputBuffer = outputBuffer;
        this.wrapperThread = new Thread(new Runnable() {
          @Override
          public void run() {
            kernel.waitForCompletion();
    //         log("  Detected completed kernel "+complete.id);

            synchronized(BufferRunner.somethingHappened) {
                toCopyFromOpenCL.add(kernel);
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
    */
}
