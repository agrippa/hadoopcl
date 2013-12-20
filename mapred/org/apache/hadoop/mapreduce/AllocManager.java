package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

public abstract class AllocManager<Type> {
    protected final int maxAllocated;
    protected int nAllocated;
    protected final LinkedList<Type> free;
    protected final Class<? extends Type> toInstantiate;
    protected final String name;
    private long timeWaiting = 0;
    protected final AtomicInteger idIncr = new AtomicInteger(0);
    protected final ReentrantLock lock;
    protected final Condition cond;

    public long timeWaiting() {
        return this.timeWaiting;
    }

    public AllocManager(String name, int setMax,
            Class<? extends Type> toInstantiate, boolean exclusive) {
        this.nAllocated = 0;
        this.maxAllocated = setMax;
        this.free = new LinkedList<Type>();
        this.toInstantiate = toInstantiate;
        this.name = name;
        if (exclusive) {
            this.lock = null;
            this.cond = null;
        } else {
            this.lock = new ReentrantLock();
            this.cond = this.lock.newCondition();
        }
    }

    private void doLock() {
      if (this.lock != null) {
        lock.lock();
      }
    }

    private void doUnlock() {
      if (this.lock != null) {
        lock.unlock();
      }
    }

    private void doSignal() {
      if (this.cond != null) {
        cond.signalAll();
      }
    }

    private void doAwait() {
      if (this.cond != null) {
        try {
          cond.await();
        } catch(InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      }
    }

    public abstract TypeAlloc<Type> nonBlockingAlloc();
    public abstract TypeAlloc<Type> alloc();
    public abstract void free(Type b);

    /**
     * Basically, how many future allocation requests we could successfully
     * respond to.
     */
    public int nAvailable() {
      int result;
      doLock();
      result = free.size() + (this.maxAllocated - this.nAllocated);
      doUnlock();
      return result;
    }

    protected TypeAlloc<Type> nonBlockingAllocHelper() {
      TypeAlloc<Type> result = null;
      boolean allocateNew = false;

      doLock();
      if (!free.isEmpty()) {
        Type nb = free.poll();
        result = new TypeAlloc<Type>(nb, false);
      } else if (this.nAllocated < this.maxAllocated) {
        allocateNew = true;
        this.nAllocated++;
      }
      doUnlock();

      if (allocateNew) {
        Type nb;
        try {
            nb = toInstantiate.newInstance();
        } catch(InstantiationException ie) {
            throw new RuntimeException(ie);
        } catch(IllegalAccessException iae) {
            throw new RuntimeException(iae);
        }
        result = new TypeAlloc<Type>(nb, true);
      }

      return result;
    }

    protected TypeAlloc<Type> allocHelper() {
        TypeAlloc<Type> result = null;
        boolean allocateNew = false;

        doLock();
        // First, we always try to re-use an old buffer
        if (!free.isEmpty()) {
            Type nb = free.poll();
            result = new TypeAlloc<Type>(nb, false);
        } else if (this.nAllocated < this.maxAllocated) {
          allocateNew = true;
          this.nAllocated++;
        } else {
          long start = System.currentTimeMillis();
          while (free.isEmpty()) {
              doAwait();
          }
          result = new TypeAlloc<Type>(free.poll(), false);
          long stop = System.currentTimeMillis();
          timeWaiting += (stop-start);
        }
        doUnlock();

        if (allocateNew) {
          try {
              result = new TypeAlloc<Type>(toInstantiate.newInstance(), true);
          } catch(InstantiationException ie) {
              throw new RuntimeException(ie);
          } catch(IllegalAccessException iae) {
              throw new RuntimeException(iae);
          }
        }

        return result;
    }

    protected void freeHelper(Type b) {
        doLock();
        free.add(b);
        doSignal();
        doUnlock();
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
