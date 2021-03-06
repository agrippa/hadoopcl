package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.mapreduce.Reducer.Context;
import com.amd.aparapi.Range;
import com.amd.aparapi.Kernel;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import java.util.List;
import java.util.ArrayList;

public abstract class HadoopCLInputReducerBuffer extends HadoopCLInputBuffer {
    public final int[] keyIndex;
    public int nKeys;
    public int nVals;
    public final int keyCapacity;
    public final int valCapacity;

    public int lastNKeys;
    public int lastNVals;

    public HadoopCLInputReducerBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
        this.keyIndex = new int[this.clContext.getInputBufferSize()];
        this.nKeys = 0;
        this.nVals = 0;
        this.keyCapacity = this.clContext.getInputBufferSize();
        this.valCapacity = this.clContext.getInputBufferSize() *
            this.clContext.getInputValMultiplier();

        this.lastNKeys = -1;
        this.lastNVals = -1;
    }

    public boolean sameAsLastKey(Object obj) {
        throw new UnsupportedOperationException();
    }

    public abstract void removeLastKey();
    public abstract void transferLastKey(HadoopCLInputReducerBuffer otherBuffer);

    public final boolean hasKeyLeftover() {
      return this.lastNKeys != -1;
    }

    public boolean hasWork() {
        return this.nKeys > 0;
    }

    @Override
    public int getNInputs() {
        return this.nKeys;
    }

    @Override
    public void addKeyAndValue(TaskInputOutputContext context)
            throws IOException, InterruptedException {
        addTypedKey(((Context)context).getCurrentKey());
        addTypedValue(((Context)context).getCurrentValue());
    }

    @Override
    public long space() {
        return super.space() + (4 * keyIndex.length);
    }

    @Override
    public final void clearNWrites() {
        for (int i = 0; i < nKeys; i++) {
            this.nWrites[i] = -1;
        }
    }

    protected final void safeTransfer(final Object src, final Object target, final int srcOffset, final int length) {
        final int srcTop = srcOffset + length;
        if (target != src) {
            System.arraycopy(src, srcOffset, target, 0, length);
        } else {
            if (length < srcOffset) {
                System.arraycopy(src, srcOffset, target, 0, length);
            } else {
                int currentSrc = srcOffset;
                int currentTarget = 0;
                while (currentSrc < srcTop) {
                    int copyable = currentSrc - currentTarget;
                    if (currentSrc + copyable > srcTop) {
                        copyable = srcTop - currentSrc;
                    }

                    System.arraycopy(src, currentSrc, target, currentTarget, copyable);
                    currentSrc += copyable;
                    currentTarget += copyable;
                }
            }
        }
    }
}
