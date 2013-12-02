package org.apache.hadoop.mapreduce;

/*
 * Useful utilities (like merge and sort) that don't rely on advanced Java
 * functionality that APARAPI can't handle.
 */
public class HadoopCLUtils {

    private static void stupidSort(int[] arr, int[] coarr, int len) {
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

    protected static int findNextSmallest(int sparseIndex, int startIndex,
            int[] queueOfSparseIndices, int[] queueOfSparseIndicesLinks) {
        int index = startIndex;
        int prev = -1;

        while (index != -1 && queueOfSparseIndices[index] <
                sparseIndex) {
            prev = index;
            index = queueOfSparseIndices[index];
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
    // public static int merge(HadoopCLFsvecValueIterator valsIter,
    //         int[] outputIndices, float[] outputVals, int totalNElements,
    //         int[] indicesIntoVectors,
    //         int[] queueOfSparseIndices, int[] queueOfVectors) {

    //     for (int i = 0; i < valsIter.nValues(); i++) {
    //         valsIter.seekTo(i);
    //         indicesIntoVectors[i] = 0;
    //         queueOfSparseIndices[i] = valsIter.getValIndices()[0];
    //         queueOfVectors[i] = i;
    //     }

    //     // Sort queueOfSparseIndices so that the vectors with the smallest minimum
    //     // index is at the front of the queue (i.e. index 0)
    //     stupidSort(queueOfSparseIndices, queueOfVectors, valsIter.nValues());

    //     // Current queue head, incremented as we pass through the queue
    //     int queueHead = 0;

    //     // The number of individual input elements we've passed over so far.
    //     int nProcessed = 0;
    //     // The number of individual output elements we've written so far.
    //     // This may be less than nProcessed if there are duplicated sparse
    //     // indices in different input vectors.
    //     int nOutput = 0;
    //     // Current length of the queue
    //     int queueLength = valsIter.nValues();

    //     // While we haven't processed all input elements.
    //     while (nProcessed < totalNElements) {

    //         // Retrieve the vector ID in the input vals which has the
    //         // smallest minimum index that hasn't been processed so far.
    //         int minVector = queueOfVectors[queueHead];
    //         boolean dontIncr = false;

    //         valsIter.seekTo(minVector);
    //         int newIndex = ++indicesIntoVectors[minVector];
    //         int minIndex = valsIter.getValIndices()[newIndex-1];
    //         float minValue = valsIter.getValVals()[newIndex-1];

    //         if (newIndex < valsIter.currentVectorLength()) {
    //             // If there are still elements to be processed in the current
    //             // vector, start by grabbing the value of the next smallest
    //             // index.
    //             int tmp = valsIter.getValIndices()[newIndex];
    //             if (tmp <= queueOfSparseIndices[forwardIterate(queueHead,
    //                         queueLength)]) {
    //                 // If the next element in the current vector is also smaller
    //                 // than any of the current elements in the queue, just place
    //                 // it back at our current location in the circular queue and
    //                 // don't increment the queueHead below.
    //                 queueOfSparseIndices[queueHead] = tmp;
    //                 dontIncr = true;
    //             } else {
    //                 // Otherwise, we need to insert our newly discovered min for
    //                 // the current vector back into the appropriate place in the
    //                 // queue.
    //                 insert(tmp, minVector,
    //                         queueOfSparseIndices, queueOfVectors,
    //                         queueLength, queueHead);
    //             }
    //         } else {
    //             // We've finished all of the elements in the current vector, so
    //             // the queue can be resized down.
    //             for (int i = queueHead + 1; i < queueLength; i++) {
    //                 queueOfSparseIndices[i-1] = queueOfSparseIndices[i];
    //                 queueOfVectors[i-1] = queueOfVectors[i];
    //             }
    //             queueLength--;
    //             /*
    //              * Decrementing queueHead will ensure that it either gets set
    //              * to the same location on the forwardIterate below, or wraps
    //              * around to the front of the queue (rather than special
    //              * casing that here).
    //              */
    //             queueHead--;
    //         }
    //         nProcessed++;

    //         // Write the values we just extracted to the output combined
    //         // values.
    //         if (nOutput > 0 && outputIndices[nOutput-1] == minIndex) {
    //             outputVals[nOutput-1] += minValue;
    //         } else {
    //             outputIndices[nOutput] = minIndex;
    //             outputVals[nOutput] = minValue;
    //             nOutput++;
    //         }

    //         // If we didn't find the next smallest index in the same vector,
    //         // need to iterate the queueHead to the next location.
    //         if (!dontIncr) {
    //             queueHead = forwardIterate(queueHead,
    //                     queueLength);
    //         }
    //     }
    //     return nOutput;
    // }

    public static int merge(HadoopCLSvecValueIterator valsIter,
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

        // Sort queueOfSparseIndices so that the vectors with the smallest
        // minimum index is at the front of the queue (i.e. index 0).
        stupidSort(queueOfSparseIndices, queueOfVectors, valsIter.nValues());

        // Current queue head, incremented as we pass through the queue
        int queueHead = 0;

        // The number of individual input elements we've passed over so far.
        int nProcessed = 0;
        // The number of individual output elements we've written so far.
        // This may be less than nProcessed if there are duplicated sparse
        // indices in different input vectors.
        int nOutput = 0;
        // Current length of the queue
        int queueLength = valsIter.nValues();

        // While we haven't processed all input elements.
        while (nProcessed < totalNElements) {

            // Retrieve the vector ID in the input vals which has the
            // smallest minimum index that hasn't been processed so far.
            int minVector = queueOfVectors[queueHead];

            valsIter.seekTo(minVector);
            int newIndex = ++indicesIntoVectors[minVector];
            int minIndex = valsIter.getValIndices()[newIndex-1];
            double minValue = valsIter.getValVals()[newIndex-1];
            int todoNext = queueOfSparseIndicesLinks[queueHead];

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
    public static int binarySearch(int[] vals, int find, int inLow, int inHigh) {
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
    public static int linearSearch(int[] vals, int find, int low, int high) {
      int i = low;
      for ( ; i < high && vals[i] < find; i++) ;
      return i < high && vals[i] == find ? i : -1;
    }
}
