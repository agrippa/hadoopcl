package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.LinkedList;

public class KernelManager extends AllocManager<HadoopCLKernel> {

    public KernelManager(String name, int setMax,
            Class<? extends HadoopCLKernel> toInstantiate,
            HadoopOpenCLContext clContext) {
        super(name, setMax, toInstantiate, clContext);
    }

    public void preallocateKernels() {
        for (int i = this.nAllocated; i < this.maxAllocated; i++) {
            HadoopCLKernel newKernel;
            try {
                newKernel = toInstantiate.newInstance();
            } catch (InstantiationException ie) {
                throw new RuntimeException(ie);
            } catch (IllegalAccessException iae) {
                throw new RuntimeException(iae);
            }
            newKernel.id = idIncr++;
            newKernel.init(this.clContext);
            newKernel.setGlobals(this.clContext.getGlobalsInd(),
                this.clContext.getGlobalsVal(),
                this.clContext.getGlobalIndices(), this.clContext.getNGlobals(),
                this.clContext.getGlobalsMapInd(), this.clContext.getGlobalsMapVal(),
                this.clContext.getGlobalsMap(),
                this.clContext.nGlobalBuckets());
            newKernel.doEntrypointInit(this.clContext.getDevice());
            this.free.add(newKernel);
            this.nAllocated++;
        }
    }

    public TypeAlloc<HadoopCLKernel> alloc() {
        TypeAlloc<HadoopCLKernel> result = this.allocHelper();
        if (result != null && result.isFresh()) {
            throw new RuntimeException(
                "Should be no fresh kernels with new entrypoint initialization");
        }
        return result;
    }

    public void free(HadoopCLKernel k) {
        this.freeHelper(k);
    }

    public void dispose() {
        for (HadoopCLKernel k : free) {
            k.dispose();
        }
    }

    private String str() {
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
