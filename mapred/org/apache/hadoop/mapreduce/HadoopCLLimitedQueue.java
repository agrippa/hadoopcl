package org.apache.hadoop.mapreduce;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.List;
import java.util.LinkedList;

public class HadoopCLLimitedQueue<BufferType extends HadoopCLBuffer> {
    private final int maxLength;
    private final LinkedList<BufferType> queue;

    public HadoopCLLimitedQueue(int maxLength) {
        this.queue = new LinkedList<BufferType>();
        this.maxLength = maxLength;
    }

    public HadoopCLLimitedQueue() {
        this.queue = new LinkedList<BufferType>();
        this.maxLength = -1;
    }

    public synchronized void add(BufferType ele) {
        if (this.maxLength == -1 || this.queue.size() < this.maxLength) {
            this.queue.add(ele);
            this.notify();
        }
    }

    public synchronized BufferTypeContainer<BufferType> nonBlockingGet() {
        BufferTypeContainer<BufferType> result = null;
        if (!queue.isEmpty()) {
            result = new BufferTypeContainer<BufferType>(queue.poll());
        }
        return result;
    }

    public synchronized BufferType blockingGet() {
        while (queue.isEmpty()) {
            try {
                this.wait();
            } catch(InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
        return queue.poll();
    }

    public List<HadoopCLSpaceAccounter> collectSpace(HadoopCLSpaceAccounter.SPACE_TYPE type) {
        List<HadoopCLSpaceAccounter> space = new LinkedList<HadoopCLSpaceAccounter>();
        for (BufferType b : queue) {
            space.add(new HadoopCLSpaceAccounter(type, b.space()));
        }
        return space;
    }

    public long space() {
        long sum = 0;
        for (BufferType b : queue) {
            sum = sum + b.space();
        }
        return sum;
    }
}
