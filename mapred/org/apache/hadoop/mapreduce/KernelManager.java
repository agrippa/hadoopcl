package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.LinkedList;

public class KernelManager extends AllocManager<HadoopCLKernel> {
    private final HadoopOpenCLContext clContext;

    public KernelManager(String name, int setMax,
            Class<? extends HadoopCLKernel> toInstantiate,
            HadoopOpenCLContext clContext) {
        super(name, setMax, toInstantiate);
        this.clContext = clContext;
    }

    public TypeAlloc<HadoopCLKernel> nonBlockingAlloc() {
        TypeAlloc<HadoopCLKernel> result = this.nonBlockingAllocHelper();
        if (result != null && result.isFresh()) {
            System.err.println("Allocing kernel "+result.obj().id);
            result.obj().init(this.clContext);
            result.obj().setGlobals(this.clContext.getGlobalsInd(),
                    this.clContext.getGlobalsVal(),
                    this.clContext.getGlobalIndices(), this.clContext.getNGlobals());
        }
        return result;
    }

    public TypeAlloc<HadoopCLKernel> alloc() {
        TypeAlloc<HadoopCLKernel> result = this.allocHelper();
        if (result.isFresh()) {
            System.err.println("Allocing kernel "+result.obj().id);
            result.obj().init(this.clContext);
            result.obj().setGlobals(this.clContext.getGlobalsInd(),
                    this.clContext.getGlobalsVal(),
                    this.clContext.getGlobalIndices(), this.clContext.getNGlobals());
        }
        return result;
    }

    public void free(HadoopCLKernel k) {
        this.freeHelper(k);
    }

    private synchronized String str() {
        StringBuffer sb = new StringBuffer();
        sb.append("[ ");
        for (HadoopCLKernel b : this.free) {
            sb.append(b.id);
            sb.append(" ");
        }
        sb.append("], ");
        sb.append(this.nAllocated);
        sb.append("/");
        sb.append(this.maxAllocated);
        return sb.toString();
    }

    @Override
    public String toString() {
        return this.str();
    }
}
