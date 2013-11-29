package org.apache.hadoop.mapreduce;

import com.amd.aparapi.Kernel;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.Range;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.HadoopCLResizableIntArray;
import org.apache.hadoop.io.HadoopCLResizableDoubleArray;
import org.apache.hadoop.io.HadoopCLResizableFloatArray;

public abstract class HadoopCLKernel extends Kernel {
    protected final static AtomicInteger idIncr = new AtomicInteger(0);
    public final int id = HadoopCLKernel.idIncr.getAndIncrement();

    protected HadoopOpenCLContext clContext;
    protected HadoopCLAccumulatedProfile javaProfile;

    protected double[] globalsVal;
    protected float[] globalsFval;
    protected int[] globalsInd;
    protected int[] globalIndices;
    protected int nGlobals;

    protected int[] globalsMapInd;
    protected double[] globalsMapVal;
    protected float[] globalsMapFval;
    protected int[] globalsMap;
    protected int nGlobalBuckets;

    protected int[] memIncr;
    protected int outputsPerInput;
    private HadoopCLResizableIntArray copyIndices = new HadoopCLResizableIntArray();
    private HadoopCLResizableDoubleArray copyVals = new HadoopCLResizableDoubleArray();
    private HadoopCLResizableFloatArray copyFvals = new HadoopCLResizableFloatArray();

    // public abstract Class getBufferClass();
    public abstract Class<? extends HadoopCLInputBuffer> getInputBufferClass();
    public abstract Class<? extends HadoopCLOutputBuffer> getOutputBufferClass();
    public abstract boolean launchKernel() throws IOException, InterruptedException;
    public abstract void init(HadoopOpenCLContext clContext);
    public abstract int getOutputPairsPerInput();
    public abstract HadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException;
    public abstract void fill(HadoopCLInputBuffer inputBuffer,
            HadoopCLOutputBuffer outputBuffer);


    public abstract void deviceStrength(DeviceStrength str);
    public abstract Device.TYPE[] validDevices();

    public void setGlobals(int[] globalsInd, double[] globalsVal,
            float[] globalsFval, int[] globalIndices, int nGlobals,
            int[] globalsMapInd, double[] globalsMapVal, float[] globalsMapFval,
            int[] globalsMap, int nBuckets) {
        this.globalIndices = globalIndices;
        this.nGlobals = nGlobals;
        this.nGlobalBuckets = nBuckets;

        this.globalsInd = globalsInd;
        this.globalsVal = globalsVal;
        this.globalsFval = globalsFval;

        this.globalsMapInd = globalsMapInd;
        this.globalsMapVal = globalsMapVal;
        this.globalsMapFval = globalsMapFval;
        this.globalsMap = globalsMap;
    }

    protected int[] getGlobalIndices(int gid) {
        int len = globalsLength(gid);
        copyIndices.ensureCapacity(len);
        System.arraycopy(this.globalsInd, this.globalIndices[gid], copyIndices.getArray(), 0, len);
        return (int[])copyIndices.getArray();
    }

    protected double[] getGlobalVals(int gid) {
        int len = globalsLength(gid);
        copyVals.ensureCapacity(len);
        System.arraycopy(this.globalsVal, this.globalIndices[gid], copyVals.getArray(), 0, len);
        return (double[])copyVals.getArray();
    }

    protected float[] getGlobalFVals(int gid) {
        int len = globalsLength(gid);
        copyFvals.ensureCapacity(len);
        System.arraycopy(this.globalsFval, this.globalIndices[gid], copyFvals.getArray(), 0, len);
        return (float[])copyFvals.getArray();
    }

    private int findSparseIndexInGlobals(int gid, int sparseIndex) {
      int globalBucketId = gid * this.nGlobalBuckets + (sparseIndex % this.nGlobalBuckets);
      return HadoopCLUtils.binarySearch(this.globalsMapInd, sparseIndex,
          this.globalsMap[globalBucketId], 
          globalBucketId == this.globalsMap.length-1 ?
            this.globalsInd.length : this.globalsMap[globalBucketId+1]);
    }

    protected double referenceGlobalVal(int gid, int sparseIndex) {
      int globalIndex = findSparseIndexInGlobals(gid, sparseIndex);
      return globalIndex == -1 ? 0.0 : this.globalsMapVal[globalIndex];
    }

    protected float referenceGlobalFval(int gid, int sparseIndex) {
      int globalIndex = findSparseIndexInGlobals(gid, sparseIndex);
      return globalIndex == -1 ? 0.0f : this.globalsMapFval[globalIndex];
    }

    protected int nGlobals() {
        return this.nGlobals;
    }

    protected int globalsLength(int gid) {
        int base = this.globalIndices[gid];
        int top = gid == nGlobals-1 ?
                this.globalsInd.length : this.globalIndices[gid + 1];
        return top - base;
    }

    public boolean doIntermediateReduction() {
        return false;
    }
    public abstract boolean equalInputOutputTypes();

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HadoopCLKernel) {
            HadoopCLKernel other = (HadoopCLKernel)obj;
            return this.id == other.id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.id;
    }
}
