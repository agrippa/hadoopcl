package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.LinkedList;

public class BufferManager<BufferType extends HadoopCLBuffer> {
    private final int maxAllocated;
    private int nAllocated;
    private final LinkedList<BufferType> free;
    private final Class<? extends BufferType> toInstantiate;
    private final String name;
    private final List<HadoopCLBuffer> globalSpace;
    private long timeWaiting = 0;

    public long timeWaiting() {
        return this.timeWaiting;
    }

    public BufferManager(String name, int setMax,
            Class<? extends BufferType> toInstantiate, List<HadoopCLBuffer> globalSpace) {
        this.nAllocated = 0;
        this.maxAllocated = setMax;
        this.free = new LinkedList<BufferType>();
        this.toInstantiate = toInstantiate;
        this.name = name;
        this.globalSpace = globalSpace;
    }

    public synchronized BufferTypeAlloc<BufferType> nonBlockingAlloc() {
        if (!free.isEmpty()) {
            BufferType nb = free.poll();
            nb.setInUse(true);
            return new BufferTypeAlloc<BufferType>(nb, false);
        } else if (this.nAllocated < this.maxAllocated) {
            BufferType result;
            try {
                result = toInstantiate.newInstance();
            } catch(InstantiationException ie) {
                throw new RuntimeException(ie);
            } catch(IllegalAccessException iae) {
                throw new RuntimeException(iae);
            }
            if (OpenCLDriver.profileMemory) {
                synchronized(globalSpace) {
                    this.globalSpace.add(result);
                }
                System.err.println("Allocating "+name+" "+(this.nAllocated+1)+"/"+this.maxAllocated);
            }
            this.nAllocated++;
            result.setInUse(true);
            return new BufferTypeAlloc<BufferType>(result, true);
        } else {
            return null;
        }
    }

    public synchronized BufferTypeAlloc<BufferType> alloc() {
        // First, we always try to re-use an old buffer
        if (!free.isEmpty()) {
            BufferType nb = free.poll();
            nb.setInUse(true);
            return new BufferTypeAlloc<BufferType>(nb, false);
        }

        // Now, if we can allocate a new object we do so
        // Otherwise, we wait for an object to be freed
        BufferType result;
        boolean isFresh;
        if (this.nAllocated < this.maxAllocated) {
            try {
                result = toInstantiate.newInstance();
            } catch(InstantiationException ie) {
                throw new RuntimeException(ie);
            } catch(IllegalAccessException iae) {
                throw new RuntimeException(iae);
            }
            if (OpenCLDriver.profileMemory) {
                synchronized(globalSpace) {
                    this.globalSpace.add(result);
                }
                System.err.println("Allocating "+name+" "+(this.nAllocated+1)+"/"+this.maxAllocated);
            }
            this.nAllocated++;
            isFresh = true;
        } else {
            long start = System.currentTimeMillis();
            while (free.isEmpty()) {
                try {
                    this.wait();
                } catch(InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
            result = free.poll();
            long stop = System.currentTimeMillis();
            timeWaiting += (stop-start);
            isFresh = false;
        }
        result.setInUse(true);
        return new BufferTypeAlloc<BufferType>(result, isFresh);
    }

    public synchronized void free(BufferType b) {
        b.setInUse(false);
        free.add(b);
        this.notify();
    }

    public static class BufferTypeAlloc<InnerBufferType> {
        private final boolean isFresh;
        private final InnerBufferType obj;

        public BufferTypeAlloc(InnerBufferType obj, boolean isFresh) {
            this.obj = obj;
            this.isFresh = isFresh;
        }

        public boolean isFresh() { return this.isFresh; }
        public InnerBufferType obj() { return this.obj; }
    }
}
