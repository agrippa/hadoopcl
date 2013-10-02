package org.apache.hadoop.mapreduce;

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

    public void resetProfile() {
        this.prof = new Profile();
    }

    public Profile getProfile() {
        return this.prof;
    }

    public void setInUse(boolean inUse) {
        this.inUse = inUse;
    }

    public boolean inUse() { return this.inUse; }

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

    public static class Profile {
        private long startRead;
        private long stopRead;
        private long startKernel;
        private long stopKernel;
        private long startWrite;
        private long stopWrite;
        private int nKernelAttempts;
        private int nItemsProcessed;

        public void addKernelAttempt() {
            this.nKernelAttempts++;
        }

        public void addItemProcessed() {
            this.nItemsProcessed++;
        }

        public void startRead() {
            this.startRead = System.currentTimeMillis();
        }

        public void stopRead() {
            this.stopRead = System.currentTimeMillis();
        }
        
        public void startKernel() {
            this.startKernel = System.currentTimeMillis();
        }

        public void stopKernel() {
            this.stopKernel = System.currentTimeMillis();
        }

        public void startWrite() {
            this.startWrite = System.currentTimeMillis();
        }

        public void stopWrite() {
            this.stopWrite = System.currentTimeMillis();
        }

        public long readTime() {
            return this.stopRead - this.startRead;
        }

        public long kernelTime() {
            return this.stopKernel - this.startKernel;
        }

        public long writeTime() {
            return this.stopWrite - this.startWrite;
        }

        public int nKernelAttempts() {
            return this.nKernelAttempts;
        }

        public int nItemsProcessed() {
            return this.nItemsProcessed;
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
    }

}
