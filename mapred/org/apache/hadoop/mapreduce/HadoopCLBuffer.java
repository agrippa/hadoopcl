package org.apache.hadoop.mapreduce;

import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;
import java.io.IOException;
import java.lang.InterruptedException;

public abstract class HadoopCLBuffer {
    protected HadoopOpenCLContext clContext;
	protected boolean keep;
    protected int[] memIncr;

    public abstract int nContents();
    public abstract void init(int pairsPerInput, HadoopOpenCLContext clContext);
    public abstract boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException;
    public abstract void reset();
    public abstract boolean hasWork();
    public abstract boolean completedAll();
    public abstract HadoopCLReducerBuffer putOutputsIntoHadoop(TaskInputOutputContext context, boolean doIntermediateReduction) throws IOException, InterruptedException;
    public abstract void addKeyAndValue(TaskInputOutputContext context) throws IOException, InterruptedException;
    public abstract void addTypedValue(Object val);
    public abstract void addTypedKey(Object key);
    public abstract void fill(HadoopCLKernel kernel);
    public abstract void transferBufferedValues(HadoopCLBuffer buffer);
    public abstract Class getKernelClass();
    public abstract boolean equalInputOutputTypes();
    public abstract HadoopCLBuffer cloneIncomplete();
	
	public boolean keep() {
		return this.keep;
	}

}
