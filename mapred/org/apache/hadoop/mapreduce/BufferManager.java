package org.apache.hadoop.mapreduce;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.LinkedList;

public class BufferManager<BufferType extends HadoopCLBuffer> extends AllocManager<BufferType> {
    private final List<HadoopCLBuffer> globalSpace;

    public BufferManager(String name, int setMax,
            /* Class<? extends BufferType> toInstantiate, */
            List<HadoopCLBuffer> globalSpace, HadoopOpenCLContext clContext,
            Constructor<? extends BufferType> constructor) {
        super(name, setMax, /* toInstantiate, */ clContext, constructor);
        this.globalSpace = globalSpace;
    }

    public TypeAlloc<BufferType> alloc() {
        TypeAlloc<BufferType> result = this.allocHelper();
        if (result != null && result.isFresh()) {
            // LOG:DIAGNOSTIC
            log("Allocating "+this.name+" buffer "+result.obj().id);
            if (OpenCLDriver.profileMemory) {
                synchronized(globalSpace) {
                    this.globalSpace.add(result.obj());
                }
            }
        }
        return result;
    }

    public void free(BufferType b) {
        this.freeHelper(b);
    }

    private synchronized String str() {
        StringBuffer sb = new StringBuffer();
        sb.append("[ ");
        for (BufferType b : this.free) {
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
