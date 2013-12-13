package org.apache.hadoop.mapreduce;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;
import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.io.SparseVectorWritable;

public abstract class HadoopCLBuffer {
    public HadoopOpenCLContext clContext;
    protected Profile prof;
    protected int isGPU;
    public int[] nWrites;
    private boolean inUse;
    protected boolean initialized = false;
    public HadoopCLGlobalId tracker;

    protected final static AtomicInteger idIncr = new AtomicInteger(0);
    public int id = -1;
    // public final int id = HadoopCLBuffer.idIncr.getAndIncrement();

    // public abstract Class getKernelClass();
    // public abstract void init(int pairsPerInput, HadoopOpenCLContext clContext);
    // public abstract boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException;
    // public abstract void reset();
    // public abstract boolean hasWork();
    // public abstract boolean completedAll();
    // public abstract HadoopCLReducerBuffer putOutputsIntoHadoop(TaskInputOutputContext context, boolean doIntermediateReduction) throws IOException, InterruptedException;
    // public abstract void addKeyAndValue(TaskInputOutputContext context) throws IOException, InterruptedException;
    // public abstract void addTypedValue(Object val);
    // public abstract void addTypedKey(Object key);
    // public abstract void fill(HadoopCLKernel kernel);
    // public abstract void transferBufferedValues(HadoopCLBuffer buffer);
    // public abstract boolean equalInputOutputTypes();
    // public abstract HadoopCLBuffer cloneIncomplete();

    public long space() {
        return 4 * nWrites.length;
    }

    public void clearNWrites() {
        for (int i = 0; i < this.nWrites.length; i++) {
            this.nWrites[i] = -1;
        }
    }

    public void resetProfile() {
        this.prof = new Profile();
    }

    public Profile getProfile() {
        if (this.prof == null) {
            throw new RuntimeException("Null profile");
        }
        return this.prof;
    }

    public void setInUse(boolean inUse) {
        if (this.inUse == inUse) {
            throw new RuntimeException("Setting buffer in use to "+inUse+" but already set to that");
        }
        this.inUse = inUse;
    }

    public boolean inUse() { return this.inUse; }
    public boolean initialized() { return this.initialized; }

    protected int requiredCapacity(int[] lookAside, int numAccumValues,
            int numAccumElements, int newLength) {
        int maxLength = 0;
        for (int index = 0; index < numAccumValues; index++) {
            int top = (index == numAccumValues - 1 ? numAccumElements : lookAside[index + 1]);
            int base = lookAside[index];
            int length = top - base;

            if (index + length > maxLength) {
                maxLength = index + length;
            }
        }
        if (lookAside.length + newLength > maxLength) {
            maxLength = lookAside.length + newLength;
        }
        return maxLength;
    }

    protected int requiredCapacity(List<SparseVectorWritable> vectors,
            SparseVectorWritable newVector) {
        List<SparseVectorWritable> tmpList = new ArrayList<SparseVectorWritable>(vectors.size() + 1);
        tmpList.addAll(vectors);
        tmpList.add(newVector);
        return requiredCapacity(tmpList);
    }

    protected int requiredCapacity(List<SparseVectorWritable> vectors) {
        int maxLength = 0;
        int index = 0;

        for(SparseVectorWritable v : vectors) {
            if (index + v.size() > maxLength) {
                maxLength = index + v.size();
            }
            index++;
        }
        return maxLength;
    }

    class Profile {
        private long startRead;
        private long stopRead;
        private final List<Long> kernelStarts = new ArrayList<Long>();
        private final List<Long> kernelStops = new ArrayList<Long>();
        private final List<Long> writeStarts = new ArrayList<Long>();
        private final List<Long> writeStops = new ArrayList<Long>();
        private int nItemsProcessed;

        public void addItemProcessed() {
            this.nItemsProcessed++;
        }

        public void startRead() {
            OpenCLDriver.logger.log("starting read of "+HadoopCLBuffer.this.tracker.toString(),
                HadoopCLBuffer.this.clContext);
            this.startRead = System.currentTimeMillis();
        }

        public void stopRead() {
            OpenCLDriver.logger.log("finishing read of "+HadoopCLBuffer.this.tracker.toString(),
                HadoopCLBuffer.this.clContext);
            this.stopRead = System.currentTimeMillis();
        }
        
        public void startKernel() {
            this.kernelStarts.add(System.currentTimeMillis());
        }

        public void stopKernel() {
            this.kernelStops.add(System.currentTimeMillis());
        }

        public void startWrite() {
            OpenCLDriver.logger.log("starting write of "+HadoopCLBuffer.this.tracker.toString(),
                HadoopCLBuffer.this.clContext);
            this.writeStarts.add(System.currentTimeMillis());
        }

        public void stopWrite() {
            OpenCLDriver.logger.log("finishing write of "+HadoopCLBuffer.this.tracker.toString(),
                HadoopCLBuffer.this.clContext);
            this.writeStops.add(System.currentTimeMillis());
        }

        public long readTime() {
            return this.stopRead - this.startRead;
        }

        public int nKernelAttempts() {
            return this.kernelStarts.size();
        }

        public long kernelTime() {
            long sum = 0;
            for (int i = 0; i < this.kernelStarts.size(); i++) {
                sum = sum + (this.kernelStops.get(i) - this.kernelStarts.get(i));
            }
            return sum;
        }

        public long writeTime() {
            long sum = 0;
            for (int i = 0; i < this.writeStarts.size(); i++) {
                sum = sum + (this.writeStops.get(i) - this.writeStarts.get(i));
            }
            return sum;
        }

        public int nItemsProcessed() {
            return this.nItemsProcessed;
        }

    }

    public static String listToString(List<Profile> profiles) {
      StringBuffer sb = new StringBuffer();
      if (profiles != null && profiles.size() > 0) {
        long accumRead = 0;
        long accumKernel = 0;
        long accumWrite = 0;
        int accumAttempts = 0;
        int nItemsProcessed = 0;
        for(HadoopCLBuffer.Profile p : profiles) {
          accumRead += p.readTime();
          accumKernel += p.kernelTime();
          accumWrite += p.writeTime();
          accumAttempts += p.nKernelAttempts();
          nItemsProcessed += p.nItemsProcessed();
        }
        sb.append(", nBuffers=");
        sb.append(profiles.size());
        sb.append(", readTime=");
        sb.append(accumRead);
        sb.append(" ms, kernelTime=");
        sb.append(accumKernel);
        sb.append(" ms, writeTime=");
        sb.append(accumWrite);
        sb.append(" ms, kernelAttempts=");
        sb.append(accumAttempts);
        sb.append(", itemsProcessed=");
        sb.append(nItemsProcessed);
      }
      return sb.toString();

    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HadoopCLBuffer) {
            HadoopCLBuffer other = (HadoopCLBuffer)obj;
            return this.id == other.id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.id;
    }

}
