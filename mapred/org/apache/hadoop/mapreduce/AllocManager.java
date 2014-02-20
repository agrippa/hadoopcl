package org.apache.hadoop.mapreduce;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

public abstract class AllocManager<Type> {
    protected final int maxAllocated;
    protected int nAllocated;
    protected final LinkedList<Type> free;
    // protected final Class<? extends Type> toInstantiate;
    protected final String name;
    protected int idIncr = 0;
    protected final HadoopOpenCLContext clContext;
    private final boolean enableLogs;
    protected final Constructor<? extends Type> constructor;

    protected void log(String s) {
        if (enableLogs) {
            System.err.println(System.currentTimeMillis()+"|"+this.clContext.verboseTypeName()+" "+s);
        }
    }

    public AllocManager(String name, int setMax,
            /* Class<? extends Type> toInstantiate, */ HadoopOpenCLContext clContext,
            Constructor<? extends Type> constructor) {
        this.nAllocated = 0;
        this.maxAllocated = setMax;
        this.free = new LinkedList<Type>();
        // this.toInstantiate = toInstantiate;
        this.name = name;
        this.enableLogs = clContext.enableBufferRunnerDiagnostics();
        this.clContext = clContext;
        this.constructor = constructor;
    }

    public abstract TypeAlloc<Type> alloc();
    public abstract void free(Type b);

    /**
     * Basically, how many future allocation requests we could successfully
     * respond to.
     */
    public int nAvailable() {
      return free.size() + (this.maxAllocated - this.nAllocated);
    }

    protected TypeAlloc<Type> allocHelper() {
      TypeAlloc<Type> result = null;

      if (!free.isEmpty()) {
        Type nb = free.poll();
        result = new TypeAlloc<Type>(nb, false);
      } else if (this.nAllocated < this.maxAllocated) {
        Type nb;
        try {
            nb = constructor.newInstance(this.clContext, this.idIncr++);
            // nb = toInstantiate.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        result = new TypeAlloc<Type>(nb, true);
        this.nAllocated++;
      }

      return result;
    }

    protected void freeHelper(Type b) {
        free.add(b);
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
