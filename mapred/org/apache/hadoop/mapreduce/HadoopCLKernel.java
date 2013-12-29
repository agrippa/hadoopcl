package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import com.amd.aparapi.Kernel;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.Range;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.HadoopCLResizableIntArray;
import org.apache.hadoop.io.HadoopCLResizableDoubleArray;
import org.apache.hadoop.io.HadoopCLResizableFloatArray;

public abstract class HadoopCLKernel extends Kernel {
    // protected final static AtomicInteger idIncr = new AtomicInteger(0);
    // public final int id = HadoopCLKernel.idIncr.getAndIncrement();
    public int id = -1;
    public HadoopCLGlobalId tracker;

    protected HadoopOpenCLContext clContext;
    protected IHadoopCLAccumulatedProfile javaProfile;
    protected HadoopCLProfile openclProfile;

    protected double[] globalsVal;
    protected float[] globalsFval;
    protected int[] globalsInd;
    protected int[] globalIndices;
    protected int nGlobals;

    protected int[] globalsMapInd;
    protected double[] globalsMapVal;
    protected float[] globalsMapFval;
    protected int[] globalsMap;
    protected int nGlobalBuckets;

    protected int[] outputIterMarkers;
    protected int[] memIncr;
    protected int[] memRetry;
    protected int outputsPerInput;
    private HadoopCLResizableIntArray copyIndices = new HadoopCLResizableIntArray();
    private HadoopCLResizableDoubleArray copyVals = new HadoopCLResizableDoubleArray();
    private HadoopCLResizableFloatArray copyFvals = new HadoopCLResizableFloatArray();
    protected final HashMap<String, Integer> arrayLengths = new HashMap<String, Integer>();

    // public abstract Class getBufferClass();
    public abstract Class<? extends HadoopCLInputBuffer> getInputBufferClass();
    public abstract Class<? extends HadoopCLOutputBuffer> getOutputBufferClass();
    public abstract boolean launchKernel() throws IOException, InterruptedException;
    public abstract boolean relaunchKernel() throws IOException, InterruptedException;
    public abstract void init(HadoopOpenCLContext clContext);
    public abstract int getOutputPairsPerInput();
    public abstract IHadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException;
    public abstract void fill(HadoopCLInputBuffer inputBuffer);
    public abstract void prepareForRead(HadoopCLOutputBuffer outputBuffer);

    public abstract void deviceStrength(DeviceStrength str);
    public abstract Device.TYPE[] validDevices();
    public abstract boolean equalInputOutputTypes();

    public boolean outOfMemory() {
        return false;
    }

    public void setGlobals(int[] globalsInd, double[] globalsVal,
            float[] globalsFval, int[] globalIndices, int nGlobals,
            int[] globalsMapInd, double[] globalsMapVal, float[] globalsMapFval,
            int[] globalsMap, int nBuckets) {
        this.globalIndices = globalIndices;
        this.nGlobals = nGlobals;
        this.nGlobalBuckets = nBuckets;

        this.globalsInd = globalsInd;
        this.globalsVal = globalsVal;
        this.globalsFval = globalsFval;

        this.globalsMapInd = globalsMapInd;
        this.globalsMapVal = globalsMapVal;
        this.globalsMapFval = globalsMapFval;
        this.globalsMap = globalsMap;
    }

    protected int[] getGlobalIndices(int gid) {
        int len = globalsLength(gid);
        copyIndices.ensureCapacity(len);
        System.arraycopy(this.globalsInd, this.globalIndices[gid], copyIndices.getArray(), 0, len);
        return (int[])copyIndices.getArray();
    }

    protected double[] getGlobalVals(int gid) {
        int len = globalsLength(gid);
        copyVals.ensureCapacity(len);
        System.arraycopy(this.globalsVal, this.globalIndices[gid], copyVals.getArray(), 0, len);
        return (double[])copyVals.getArray();
    }

    protected float[] getGlobalFVals(int gid) {
        int len = globalsLength(gid);
        copyFvals.ensureCapacity(len);
        System.arraycopy(this.globalsFval, this.globalIndices[gid], copyFvals.getArray(), 0, len);
        return (float[])copyFvals.getArray();
    }

    private int findSparseIndexInGlobals(int gid, int sparseIndex) {
      int globalBucketId = gid * this.nGlobalBuckets + (sparseIndex % this.nGlobalBuckets);
      return binarySearch(this.globalsMapInd, sparseIndex,
          this.globalsMap[globalBucketId], 
          globalBucketId == this.globalsMap.length-1 ?
            this.globalsInd.length : this.globalsMap[globalBucketId+1]);
      // return HadoopCLUtils.linearSearch(this.globalsMapInd, sparseIndex,
      //     this.globalsMap[globalBucketId], 
      //     globalBucketId == this.globalsMap.length-1 ?
      //       this.globalsInd.length : this.globalsMap[globalBucketId+1]);
    }

    protected double referenceGlobalVal(int gid, int sparseIndex) {
      int globalIndex = findSparseIndexInGlobals(gid, sparseIndex);
      return globalIndex == -1 ? 0.0 : this.globalsMapVal[globalIndex];
    }

    protected float referenceGlobalFval(int gid, int sparseIndex) {
      int globalIndex = findSparseIndexInGlobals(gid, sparseIndex);
      return globalIndex == -1 ? 0.0f : this.globalsMapFval[globalIndex];
    }

    protected int nGlobals() {
        return this.nGlobals;
    }

    protected int globalsLength(int gid) {
        int base = this.globalIndices[gid];
        int top = gid == nGlobals-1 ?
                this.globalsInd.length : this.globalIndices[gid + 1];
        return top - base;
    }

    public boolean doIntermediateReduction() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HadoopCLKernel) {
            HadoopCLKernel other = (HadoopCLKernel)obj;
            return this.id == other.id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.id;
    }

    /**
     * Utilities for HadoopCL Kernels.
     */
    protected void quickSort(int[] arr, double[] coarr, int elements, int[] beg, int[] end) {
      int piv, L, R, swap;
      double dpiv;
      int i = 0;

      beg[0] = 0;
      end[0] = elements;

      while (i >= 0) {
        L=beg[i]; R=end[i]-1;
        if (L < R) {
          piv = arr[L];
          dpiv = coarr[L];

          while (L < R) {
            while (arr[R]>=piv && L<R) R--;
            if (L<R) {
              coarr[L] = coarr[R];
              arr[L++]=arr[R];
            }
            while (arr[L]<=piv && L<R) L++;
            if (L<R) {
              coarr[R] = coarr[L];
              arr[R--]=arr[L];
            }
          }

          arr[L]=piv; coarr[L] = dpiv;
          beg[i+1]=L+1; end[i+1]=end[i]; end[i++]=L;
          if (end[i]-beg[i]>end[i-1]-beg[i-1]) {
            swap=beg[i]; beg[i]=beg[i-1]; beg[i-1]=swap;
            swap=end[i]; end[i]=end[i-1]; end[i-1]=swap;
          }
        } else {
          i--;
        }
      }
    }

    protected void quickSort(int[] arr, int[] coarr, int elements, int[] beg, int[] end) {
      int piv, L, R, swap;
      int dpiv;
      int i = 0;

      beg[0] = 0;
      end[0] = elements;

      while (i >= 0) {
        L=beg[i]; R=end[i]-1;
        if (L < R) {
          piv = arr[L];
          dpiv = coarr[L];

          while (L < R) {
            while (arr[R]>=piv && L<R) R--;
            if (L<R) {
              coarr[L] = coarr[R];
              arr[L++]=arr[R];
            }
            while (arr[L]<=piv && L<R) L++;
            if (L<R) {
              coarr[R] = coarr[L];
              arr[R--]=arr[L];
            }
          }

          arr[L]=piv; coarr[L] = dpiv;
          beg[i+1]=L+1; end[i+1]=end[i]; end[i++]=L;
          if (end[i]-beg[i]>end[i-1]-beg[i-1]) {
            swap=beg[i]; beg[i]=beg[i-1]; beg[i-1]=swap;
            swap=end[i]; end[i]=end[i-1]; end[i-1]=swap;
          }
        } else {
          i--;
        }
      }
    }

    protected void stupidSort(int[] arr, double[] coarr, int len) {
        for (int i = 0; i < len; i++) {
            int minIndex = i;
            int minVal = arr[i];
            for (int j = i + 1; j < len; j++) {
                if (arr[j] < minVal) {
                    minIndex = j;
                    minVal = arr[j];
                }
            }
            arr[minIndex] = arr[i];
            arr[i] = minVal;

            double tmp = coarr[i];
            coarr[i] = coarr[minIndex];
            coarr[minIndex] = tmp;
        }
    }

    private void stupidSort(int[] arr, int[] coarr, int len) {
        for (int i = 0; i < len; i++) {
            int minIndex = i;
            int minVal = arr[i];
            for (int j = i + 1; j < len; j++) {
                if (arr[j] < minVal) {
                    minIndex = j;
                    minVal = arr[j];
                }
            }
            arr[minIndex] = arr[i];
            arr[i] = minVal;

            int tmp = coarr[i];
            coarr[i] = coarr[minIndex];
            coarr[minIndex] = tmp;
        }
    }

    /**
     * Aparapi generates an incorrect version of this, so the definition of this
     * method is replaced during OpenCL kernel generation
     */
    protected int findNextSmallest(int sparseIndex, int startIndex,
            int[] queueOfSparseIndices, int[] queueOfSparseIndicesLinks) {
        int index = startIndex;
        int prev = -1;

        while (index != -1 && queueOfSparseIndices[index] <
                sparseIndex) {
            prev = index;
            index = queueOfSparseIndicesLinks[index];
        }

        return prev;
    }

    protected int findEnd(int startIndex, int[] queueOfSparseIndicesLinks) {
      int index = startIndex;
      int prev = -1;
      while (index != -1) {
        prev = index;
        index = queueOfSparseIndicesLinks[index];
      }
      return prev;
    }
    /*
     * 1. outputIndices and outputVals should be as long as the number of
     *    unique indices in the vectors referenced by valsIter.
     * 2. totalNElements should be the sum of all the vector lengths in
     *    valsIter.
     * 3. vectorIndices, queueOfOffsets, and queueOfVectors should all be as
     *    long as the number of vectors in valIters.
     *
     * The output of this function will be a merged sparse vector in
     * outputIndices and outputVals where for index i the value v is the sum
     * of all elements in the input vectors that are associated with index
     * i. This will also return the length of the merged vector. Sparse
     * vectors with INT_MAX as an index value may not be passed to this
     * method.
     */
    public int merge(HadoopCLSvecValueIterator valsIter,
            int[] outputIndices, double[] outputVals, int totalNElements,
            int[] indicesIntoVectors,
            int[] queueOfSparseIndices, int[] queueOfSparseIndicesLinks,
            int[] queueOfVectors) {

        for (int i = 0; i < valsIter.nValues(); i++) {
            valsIter.seekTo(i);
            indicesIntoVectors[i] = 0;
            queueOfSparseIndices[i] = valsIter.getValIndices()[0];
            queueOfVectors[i] = i;
            queueOfSparseIndicesLinks[i] = i+1;
        }
        queueOfSparseIndicesLinks[valsIter.nValues()-1] = -1;
        // The number of individual output elements we've written so far.
        // This may be less than nProcessed if there are duplicated sparse
        // indices in different input vectors.
        int nOutput = 0;

        // Sort queueOfSparseIndices so that the vectors with the smallest
        // minimum index is at the front of the queue (i.e. index 0).
        stupidSort(queueOfSparseIndices, queueOfVectors, valsIter.nValues());

        // Current queue head, incremented as we pass through the queue
        int queueHead = 0;

        // The number of individual input elements we've passed over so far.
        int nProcessed = 0;
        // Current length of the queue
        int todoNext = 0;

        // While we haven't processed all input elements.
        while (nProcessed < totalNElements) {

            // Retrieve the vector ID in the input vals which has the
            // smallest minimum index that hasn't been processed so far.
            int minVector = queueOfVectors[queueHead];

            valsIter.seekTo(minVector);
            int newIndex = indicesIntoVectors[minVector]+1;
            int minIndex = valsIter.getValIndices()[newIndex-1];
            double minValue = valsIter.getValVals()[newIndex-1];
            indicesIntoVectors[minVector] = newIndex;
            todoNext = queueOfSparseIndicesLinks[queueHead];

            if (newIndex < valsIter.currentVectorLength()) {
                // If there are still elements to be processed in the current
                // vector, start by grabbing the value of the next smallest
                // index.
                int nextIndexInVector = valsIter.getValIndices()[newIndex];

                int indexToInsertAfter = findNextSmallest(nextIndexInVector,
                        queueHead,
                        queueOfSparseIndices, queueOfSparseIndicesLinks);
                int next = queueOfSparseIndicesLinks[indexToInsertAfter];

                // Don't need to update queueOfVectors, stays the same value
                queueOfSparseIndices[queueHead] = nextIndexInVector;
                if (indexToInsertAfter != queueHead) {
                    queueOfSparseIndicesLinks[queueHead] = next;
                    queueOfSparseIndicesLinks[indexToInsertAfter] = queueHead;
                } else {
                    todoNext = queueHead;
                }
            } else {
                // This slot is no longer valid, if we arrive at it we want to
                // crash
                queueOfSparseIndicesLinks[queueHead] = -1;
                // queueOfSparseIndices[queueHead] = Integer.MAX_VALUE;
            }
            nProcessed++;

            // Write the values we just extracted to the output combined
            // values.
            if (nOutput > 0 && outputIndices[nOutput-1] == minIndex) {
                outputVals[nOutput-1] += minValue;
            } else {
                outputIndices[nOutput] = minIndex;
                outputVals[nOutput] = minValue;
                nOutput++;
            }

            // If we didn't find the next smallest index in the same vector,
            // need to iterate the queueHead to the next location.
            queueHead = todoNext;
        }

        return nOutput;
    }
   
    /*
     * Search a sorted integer array for a given value 'find' within the bounds
     * of [low,high). Return the index in the array of the element, or -1 if not
     * found.
     */
    public int binarySearch(int[] vals, int find, int inLow, int inHigh) {
      int low = inLow;
      int high = inHigh-1;
 
      while (low <= high) {
        int mid = (high + low) / 2;
        int v = vals[mid];
        if (v == find) return mid;
        if (v > find) high = mid-1;
        else low = mid+1;
      }
      return -1;
    }

    /*
     * Search a sorted integer array for a given value 'find' within the bounds
     * of [low,high). Return the index in the array of the element, or -1 if not
     * found.
     */
    public int linearSearch(int[] vals, int find, int low, int high) {
      int i = low;
      for ( ; i < high && vals[i] < find; i++) ;
      return i < high && vals[i] == find ? i : -1;
    }

    @Override
    public TaskType checkTaskType() {
      if (this.clContext.isMapper()) {
        return TaskType.MAPPER;
      } else if (this.clContext.isCombiner()) {
        return TaskType.COMBINER;
      } else {
        return TaskType.REDUCER;
      }
    }

    @Override
    public int getArrayLength(String inArr) {
        if (!this.arrayLengths.containsKey(inArr)) {
            throw new RuntimeException("Querying for array length of invalid array "+inArr);
        }
        return this.arrayLengths.get(inArr);
    }
}
