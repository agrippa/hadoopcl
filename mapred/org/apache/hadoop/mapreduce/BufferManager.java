package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.LinkedList;

public class BufferManager<BufferType extends HadoopCLBuffer> extends AllocManager<BufferType> {
    private final List<HadoopCLBuffer> globalSpace;

    public BufferManager(String name, int setMax,
            Class<? extends BufferType> toInstantiate, List<HadoopCLBuffer> globalSpace) {
        super(name, setMax, toInstantiate);
        this.globalSpace = globalSpace;
    }

    public TypeAlloc<BufferType> nonBlockingAlloc() {
        TypeAlloc<BufferType> result = this.nonBlockingAllocHelper();
        if (result != null) {
            if (result.obj() != null) {
                result.obj().setInUse(true);
            }
            if (result.isFresh()) {
                result.obj().id = idIncr.getAndIncrement();
                System.err.println("Allocating "+this.name+" buffer "+result.obj().id);
                if (OpenCLDriver.profileMemory) {
                    synchronized(globalSpace) {
                        this.globalSpace.add(result.obj());
                    }
                }
            }
        }
        return result;
    }

    public TypeAlloc<BufferType> alloc() {
        TypeAlloc<BufferType> result = this.allocHelper();
        result.obj().setInUse(true);
        if (result.isFresh()) {
            result.obj().id = idIncr.getAndIncrement();
            System.err.println("Allocating "+this.name+" buffer "+result.obj().id);
            if (OpenCLDriver.profileMemory) {
                synchronized(globalSpace) {
                    this.globalSpace.add(result.obj());
                }
            }
        }
        return result;
    }

    public void free(BufferType b) {
        b.setInUse(false);
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
