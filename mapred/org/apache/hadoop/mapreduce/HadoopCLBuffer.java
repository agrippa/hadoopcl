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
    public final HadoopOpenCLContext clContext;
    protected HadoopCLProfile prof;
    protected final int isGPU;
    public final int[] nWrites;
    public HadoopCLGlobalId tracker;

    protected final static AtomicInteger idIncr = new AtomicInteger(0);
    public final int id;

    public HadoopCLBuffer(HadoopOpenCLContext clContext, Integer id) {
        this.clContext = clContext;
        this.isGPU = this.clContext.isGPU();
        this.id = id.intValue();
        this.nWrites = new int[this.clContext.getInputBufferSize()];
    }

    public abstract boolean completedAll();
    public abstract void printContents();

    public long space() {
        return 4 * nWrites.length;
    }

    public void clearNWrites() {
        for (int i = 0; i < this.nWrites.length; i++) {
            this.nWrites[i] = -1;
        }
    }

    public void resetProfile() {
        if (clContext.doHighLevelProfiling()) {
            this.prof = new Profile();
        } else {
            this.prof = new HadoopCLEmptyProfile(this.clContext);
        }
    }

    public HadoopCLProfile getProfile() {
        if (this.prof == null) {
            throw new RuntimeException("Null profile");
        }
        return this.prof;
    }

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

    class Profile implements HadoopCLProfile {
        private long startRead;
        private long stopRead;
        private final List<Long> kernelStarts = new ArrayList<Long>();
        private final List<Long> kernelStops = new ArrayList<Long>();
        private final List<Long> writeStarts = new ArrayList<Long>();
        private final List<Long> writeStops = new ArrayList<Long>();
        private int nItemsProcessed;

        public void addItemsProcessed(int count) {
            this.nItemsProcessed += count;
        }

        public void startRead(HadoopCLBuffer owner) {
            // LOG:PROFILE
            // OpenCLDriver.logger.log("starting read of "+owner.tracker.toString(), HadoopCLBuffer.this.clContext);
            this.startRead = System.currentTimeMillis();
        }

        public void stopRead(HadoopCLBuffer owner) {
            // LOG:PROFILE
            // OpenCLDriver.logger.log("finishing read of "+owner.tracker.toString(), HadoopCLBuffer.this.clContext);
            this.stopRead = System.currentTimeMillis();
        }
        
        public void startKernel() {
            this.kernelStarts.add(System.currentTimeMillis());
        }

        public void stopKernel() {
            this.kernelStops.add(System.currentTimeMillis());
        }

        public void startWrite(HadoopCLBuffer owner) {
            // LOG:PROFILE
            // OpenCLDriver.logger.log("starting write of "+owner.tracker.toString(), HadoopCLBuffer.this.clContext);
            this.writeStarts.add(System.currentTimeMillis());
        }

        public void stopWrite(HadoopCLBuffer owner) {
            // LOG:PROFILE
            // OpenCLDriver.logger.log("finishing write of "+owner.tracker.toString(), HadoopCLBuffer.this.clContext);
            this.writeStops.add(System.currentTimeMillis());
        }

        public long readTime() {
            return this.stopRead - this.startRead;
        }

        public int nKernelAttempts() {
            return this.kernelStarts.size();
        }

        public long kernelTime() {
            if (this.kernelStops.size() != this.kernelStarts.size()) {
                throw new RuntimeException("Write mismatch: starts "+this.kernelStarts.size()+" stops "+this.kernelStops.size());
            }

            long sum = 0;
            for (int i = 0; i < this.kernelStarts.size(); i++) {
                sum = sum + (this.kernelStops.get(i) - this.kernelStarts.get(i));
            }
            return sum;
        }

        public long writeTime() {
            if (this.writeStops.size() != this.writeStarts.size()) {
                throw new RuntimeException("Write mismatch: starts "+this.writeStarts.size()+" stops "+this.writeStops.size());
            }

            long sum = 0;
            for (int i = 0; i < this.writeStarts.size(); i++) {
                sum = sum + (this.writeStops.get(i) - this.writeStarts.get(i));
            }
            return sum;
        }

        public int nItemsProcessed() {
            return this.nItemsProcessed;
        }

        public String listToString(List<HadoopCLProfile> profiles) {
          StringBuffer sb = new StringBuffer();
          if (profiles != null && profiles.size() > 0) {
            long accumRead = 0;
            long accumKernel = 0;
            long accumWrite = 0;
            int accumAttempts = 0;
            int nItemsProcessed = 0;
            for(HadoopCLProfile p : profiles) {
              accumRead += p.readTime();
              accumKernel += p.kernelTime();
              accumWrite += p.writeTime();
              accumAttempts += p.nKernelAttempts();
              nItemsProcessed += p.nItemsProcessed();
            }
            sb.append(", nBuffers = ");
            sb.append(profiles.size());
            sb.append(", readTime = ");
            sb.append(accumRead);
            sb.append(" ms, kernelTime = ");
            sb.append(accumKernel);
            sb.append(" ms, writeTime = ");
            sb.append(accumWrite);
            sb.append(" ms, kernelAttempts = ");
            sb.append(accumAttempts);
            sb.append(", itemsProcessed = ");
            sb.append(nItemsProcessed);
          }
          return sb.toString();

        }
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

    protected String sparseVecComponentsToString(int[] vals, int offset, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
          sb.append(vals[offset+i]);
          sb.append(" ");
        }
        return sb.toString();
    }

    protected String sparseVecComponentsToString(int[] indices, int indicesOffset, float[] vals, int valsOffset, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
          sb.append(indices[indicesOffset+i]);
          sb.append(":");
          sb.append(vals[valsOffset+i]);
          sb.append(" ");
        }
        return sb.toString();
    }

    protected String sparseVecComponentsToString(int[] indices, int indicesOffset, double[] vals, int valsOffset, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
          sb.append(indices[indicesOffset+i]);
          sb.append(":");
          sb.append(vals[valsOffset+i]);
          sb.append(" ");
        }
        return sb.toString();
    }

}
