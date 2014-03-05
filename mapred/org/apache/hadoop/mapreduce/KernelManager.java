package org.apache.hadoop.mapreduce;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.LinkedList;

public class KernelManager extends AllocManager<HadoopCLKernel> {

    public KernelManager(String name, int setMax,
            HadoopOpenCLContext clContext,
            Constructor<? extends HadoopCLKernel> constructor) {
        super(name, setMax, clContext, constructor);
    }

    public void preallocateKernels() {
        for (int i = this.nAllocated; i < this.maxAllocated; i++) {
            HadoopCLKernel newKernel;
            try {
                newKernel = constructor.newInstance(this.clContext, this.idIncr++);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            newKernel.doEntrypointInit(this.clContext.getDevice(),
                this.clContext.getDeviceSlot(),
                this.clContext.getContext().getTaskAttemptID().getTaskID().getId(),
                this.clContext.getContext().getTaskAttemptID().getId());
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
