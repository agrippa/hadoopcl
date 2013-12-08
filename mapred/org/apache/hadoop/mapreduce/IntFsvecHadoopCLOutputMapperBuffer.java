package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import com.amd.aparapi.Range;
import com.amd.aparapi.Kernel;
import org.apache.hadoop.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class IntFsvecHadoopCLOutputMapperBuffer extends HadoopCLOutputMapperBuffer {
    public int[] outputKeys;
    public int[] outputValIntLookAsideBuffer;
    public int[] outputValFloatLookAsideBuffer;
    public int[] outputValIndices;
    public float[] outputValVals;
    private int[] bufferOutputIndices = null;
    private float[] bufferOutputVals = null;
    public int[] outputValLengthBuffer;
    protected int outputsPerInput;
    protected int[] memAuxIntIncr;
    protected int[] memAuxFloatIncr;
    private final int lockingInterval = 256;


    @Override
    public int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar) throws IOException, InterruptedException {
        final IntWritable saveKey = new IntWritable();
        final FSparseVectorWritable saveVal = new FSparseVectorWritable();
        int count;
        if(this.memIncr[0] < 0 || this.outputValIntLookAsideBuffer.length < this.memIncr[0]) {
            count = this.outputValIntLookAsideBuffer.length;
        } else {
            count = this.memIncr[0];
        }
              for (int i = 0; i < count; i++) {
                int intStartOffset = this.outputValIntLookAsideBuffer[i];
                int floatStartOffset = this.outputValFloatLookAsideBuffer[i];
                int length = this.outputValLengthBuffer[i];
                saveVal.set(this.outputValIndices, intStartOffset, outputValVals, floatStartOffset, length);
                saveKey.set(this.outputKeys[i]);
                context.write(saveKey, saveVal);
            }
        return -1;
    }

    @Override
    public void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext) {
        baseInit(clContext);
        this.outputsPerInput = outputsPerInput;

        if (this.outputsPerInput < 0) {
            outputKeys = new int[this.clContext.getBufferSize() * 5];
            outputValIntLookAsideBuffer = new int[this.clContext.getBufferSize() * 5];

            outputValFloatLookAsideBuffer = new int[this.clContext.getBufferSize() * 5];

            int bigger = this.clContext.getPreallocLength() > (this.clContext.getBufferSize() * 5) * 5 ? this.clContext.getPreallocLength() : (this.clContext.getBufferSize() * 5) * 5;

            outputValIndices = new int[bigger];

            outputValVals = new float[bigger];

            outputValLengthBuffer = new int[this.clContext.getBufferSize() * outputsPerInput];
            memAuxIntIncr = new int[1];
            memAuxFloatIncr = new int[1];
        } else {
            outputKeys = new int[this.clContext.getBufferSize() * outputsPerInput];
            outputValIntLookAsideBuffer = new int[this.clContext.getBufferSize() * outputsPerInput];

            outputValFloatLookAsideBuffer = new int[this.clContext.getBufferSize() * outputsPerInput];

            int bigger = this.clContext.getPreallocLength() > (this.clContext.getBufferSize() * outputsPerInput) * 5 ? this.clContext.getPreallocLength() : (this.clContext.getBufferSize() * outputsPerInput) * 5;

            outputValIndices = new int[bigger];

            outputValVals = new float[bigger];

            outputValLengthBuffer = new int[this.clContext.getBufferSize() * outputsPerInput];
            memAuxIntIncr = new int[1];
            memAuxFloatIncr = new int[1];
        }
        this.initialized = true;
    }

    @Override
    public long space() {
        return super.space() + 
            (outputKeys.length * 4) +
            (outputValIntLookAsideBuffer.length * 4) +
            (outputValFloatLookAsideBuffer.length * 4) +
            (outputValIndices.length * 4) +
            (outputValVals.length * 8) +
            (outputValLengthBuffer.length * 4) +
            (bufferOutputIndices == null ? 0 : bufferOutputIndices.length * 4) +
            (bufferOutputVals == null ? 0 : bufferOutputVals.length * 8) +
            (memAuxIntIncr.length * 4) +
            (memAuxFloatIncr.length * 4);
    }

}

