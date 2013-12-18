package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AllocManager<Type> {
    protected final int maxAllocated;
    protected int nAllocated;
    protected final LinkedList<Type> free;
    protected final Class<? extends Type> toInstantiate;
    protected final String name;
    private long timeWaiting = 0;
    protected final AtomicInteger idIncr = new AtomicInteger(0);

    public long timeWaiting() {
        return this.timeWaiting;
    }

    public AllocManager(String name, int setMax,
            Class<? extends Type> toInstantiate) {
        this.nAllocated = 0;
        this.maxAllocated = setMax;
        this.free = new LinkedList<Type>();
        this.toInstantiate = toInstantiate;
        this.name = name;
    }

    public abstract TypeAlloc<Type> nonBlockingAlloc();
    public abstract TypeAlloc<Type> alloc();
    public abstract void free(Type b);

    /**
     * Basically, how many future allocation requests we could successfully
     * respond to.
     */
    public synchronized int nAvailable() {
      return free.size() + (this.maxAllocated - this.nAllocated);
    }

    protected synchronized TypeAlloc<Type> nonBlockingAllocHelper() {
        if (!free.isEmpty()) {
            Type nb = free.poll();
            return new TypeAlloc<Type>(nb, false);
        } else if (this.nAllocated < this.maxAllocated) {
            Type result;
            try {
                result = toInstantiate.newInstance();
            } catch(InstantiationException ie) {
                throw new RuntimeException(ie);
            } catch(IllegalAccessException iae) {
                throw new RuntimeException(iae);
            }
            this.nAllocated++;
            return new TypeAlloc<Type>(result, true);
        } else {
            return null;
        }
    }

    protected synchronized TypeAlloc<Type> allocHelper() {
        // First, we always try to re-use an old buffer
        if (!free.isEmpty()) {
            Type nb = free.poll();
            return new TypeAlloc<Type>(nb, false);
        }

        // Now, if we can allocate a new object we do so
        // Otherwise, we wait for an object to be freed
        Type result;
        boolean isFresh;
        if (this.nAllocated < this.maxAllocated) {
            try {
                result = toInstantiate.newInstance();
            } catch(InstantiationException ie) {
                throw new RuntimeException(ie);
            } catch(IllegalAccessException iae) {
                throw new RuntimeException(iae);
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
        return new TypeAlloc<Type>(result, isFresh);
    }

    protected synchronized void freeHelper(Type b) {
        free.add(b);
        this.notify();
    }

    public static class TypeAlloc<InnerBufferType> {
        private final boolean isFresh;
        private final InnerBufferType obj;

        public TypeAlloc(InnerBufferType obj, boolean isFresh) {
            this.obj = obj;
            this.isFresh = isFresh;
        }

        public boolean isFresh() { return this.isFresh; }
        public InnerBufferType obj() { return this.obj; }
    }
}
