package org.apache.hadoop.mapreduce;

import java.io.IOException;

public abstract class HadoopCLOutputBuffer extends HadoopCLBuffer {
    public int[] memIncr;

    public abstract void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext);
    public abstract void putOutputsIntoHadoop(TaskInputOutputContext context) throws IOException, InterruptedException;

    public abstract void copyOverFromInput(HadoopCLInputBuffer inputBuffer);

    @Override
    public long space() {
        return super.space() + (4 * memIncr.length);
    }
}
