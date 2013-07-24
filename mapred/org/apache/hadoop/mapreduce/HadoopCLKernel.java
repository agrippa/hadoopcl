package org.apache.hadoop.mapreduce;

import com.amd.aparapi.Kernel;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.Range;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.HashMap;

import org.apache.hadoop.io.HadoopCLResizableIntArray;
import org.apache.hadoop.io.HadoopCLResizableDoubleArray;

public abstract class HadoopCLKernel extends Kernel {
    protected HadoopOpenCLContext clContext;
    //protected TaskInputOutputContext context;
    protected double[] globalsVal;
    protected int[] globalsInd;
    protected int[] globalIndices;
    protected int nGlobals;
    protected int[] memIncr;
    private HadoopCLResizableIntArray copyIndices = new HadoopCLResizableIntArray();
    private HadoopCLResizableDoubleArray copyVals = new HadoopCLResizableDoubleArray();

    public abstract Class getBufferClass();
    public abstract void launchKernel(ProfileContext profiler) throws IOException, InterruptedException;
    public abstract void init(HadoopOpenCLContext clContext);
    protected abstract int getOutputPairsPerInput();
    public abstract void javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException;

    public abstract void deviceStrength(DeviceStrength str);
    public abstract Device.TYPE[] validDevices();

    public void setGlobals(int[] globalsInd, double[] globalsVal,
            int[] globalIndices, int nGlobals) {
        this.globalsInd = globalsInd;
        this.globalsVal = globalsVal;
        this.globalIndices = globalIndices;
        this.nGlobals = nGlobals;
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
}