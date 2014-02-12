package org.apache.hadoop.mapreduce;

import java.util.Deque;
import org.apache.hadoop.mapreduce.BufferRunner.OutputBufferSoFar;
import java.util.Stack;
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
import java.util.TreeSet;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.io.DataOutputStream;
import org.apache.hadoop.io.ReadArrayUtils;
import org.apache.hadoop.mapreduce.Mapper.Context;

public final class IntUPairHadoopCLOutputMapperBuffer extends HadoopCLOutputMapperBuffer implements KVCollection<IntWritable, UniquePairWritable> {
    public  int[] outputKeys;
    public  int[] outputValIds;
    public  double[] outputVals1;
    public  double[] outputVals2;
    private int start, end;

    @Override
    public final int start() { return this.start; }
    @Override
    public final int end() { return this.end; }
    @Override
    public final boolean isValid(int index) {
        return this.itersFinished.contains(this.outputIterMarkers[index]);
    }
    @Override
    public final Iterator<Integer> iterator() {
        return new OutputIterator(this.start, this.end, this.itersFinished, this.outputIterMarkers);
    }
    @Override
    public final Writable getKeyFor(int index, Writable genericRef) {
        final IntWritable ref = (IntWritable)genericRef;
        final IntWritable out;
        if (ref != null) {
            ref.set(this.outputKeys[index]);
            out = ref;
        } else {
            out = new IntWritable(this.outputKeys[index]);
        }
        return out;
    }

    @Override
    public final Writable getValueFor(int index, Writable genericRef) {
        final UniquePairWritable ref = (UniquePairWritable)genericRef;
        final UniquePairWritable out;
        if (ref != null) {
            ref.set(this.outputValIds[index], this.outputVals1[index], this.outputVals2[index]);
            out = ref;
        } else {
            out = new UniquePairWritable(this.outputValIds[index], this.outputVals1[index], this.outputVals2[index]);
        }
        return out;
    }
    @Override
    public final void serializeKey(int index, DataOutputStream out) throws IOException {
        out.writeInt(this.outputKeys[index]);
    }
    @Override
    public final void serializeValue(int index, DataOutputStream out) throws IOException {
        out.writeInt(this.outputValIds[index]);
        out.writeDouble(this.outputVals1[index]);
        out.writeDouble(this.outputVals2[index]);
    }


    @Override
    public final int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar) throws IOException, InterruptedException {
        this.start = soFar; this.end = this.memIncr[0];
        return context.writeCollection(this);
    }
    @Override
    public Class<?> getOutputKeyClass() { return IntWritable.class; }
    @Override
    public Class<?> getOutputValClass() { return UniquePairWritable.class; }

    public IntUPairHadoopCLOutputMapperBuffer(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);

            outputKeys = new int[this.clContext.getOutputBufferSize()];
            outputValIds = new int[this.clContext.getOutputBufferSize()];
            outputVals1 = new double[this.clContext.getOutputBufferSize()];
            outputVals2 = new double[this.clContext.getOutputBufferSize()];
    }

    @Override
    public long space() {
        return super.space() + 
            (outputKeys.length * 4) +
            (outputVals1.length * 8) +
            (outputVals2.length * 8) +
            (outputValIds.length * 4);
    }

    @Override
    public final int getPartitionFor(int index, int numReduceTasks) {
        return (this.outputKeys[index] & Integer.MAX_VALUE) % numReduceTasks;
    }


    public static class KeyValueIterator extends HadoopCLKeyValueIterator {
        protected IntUPairHadoopCLOutputMapperBuffer[] buffers;
        private final IntUPairHadoopCLOutputMapperBuffer buf;

        public KeyValueIterator(Deque<OutputBufferSoFar> toWrite, final int numReduceTasks,
                final IntUPairHadoopCLOutputMapperBuffer buf) {
            this.buf = buf;
            this.buffers = new IntUPairHadoopCLOutputMapperBuffer[toWrite.size()];
            int count = 0;
            for (OutputBufferSoFar tmp : toWrite) {
                this.buffers[count++] = (IntUPairHadoopCLOutputMapperBuffer)tmp.buffer();
            }
            this.sortedIndices = new TreeSet<IntegerPair>(new Comparator<IntegerPair>() {
                @Override
                public int compare(IntegerPair a, IntegerPair b) {
                    final IntUPairHadoopCLOutputMapperBuffer aBuf = buffers[a.buffer];
                    final IntUPairHadoopCLOutputMapperBuffer bBuf = buffers[b.buffer];
                    int aPart = aBuf.getPartitionFor(a.index, numReduceTasks);
                    int bPart = bBuf.getPartitionFor(b.index, numReduceTasks);
                    if (aPart != bPart) return aPart - bPart;
                    return (aBuf.outputKeys[a.index] == bBuf.outputKeys[b.index] ? 0 : (aBuf.outputKeys[a.index] > bBuf.outputKeys[b.index] ? 1 : -1 ));
                }
                @Override
                public boolean equals(Object i) {
                    throw new UnsupportedOperationException();
                }
            });

            count = 0;
            for (OutputBufferSoFar tmp : toWrite) {
                final HadoopCLOutputBuffer tmpBuf = tmp.buffer();
                final int soFar = tmp.soFar();
                for (int i = soFar; i < tmpBuf.memIncr[0]; i++) {
                    if (!tmpBuf.itersFinished.contains(tmpBuf.outputIterMarkers[i])) continue;
                    sortedIndices.add(new IntegerPair(count, i));
                }
            count++;
            }
        }

        public ByteBuffer getKeyFor(IntegerPair index) throws IOException {
            final IntUPairHadoopCLOutputMapperBuffer buffer = buffers[index.buffer];
            this.keyBytes = resizeByteBuffer(this.keyBytes, 4);
            this.keyBytes.position(0);
            this.keyBytes.putInt(buffer.outputKeys[index.index]);
            return this.keyBytes;
        }

        public int getLengthForKey(IntegerPair index) {
            return 4;
        }

        @Override
        public final DataInputBuffer getKey() throws IOException {
            final ByteBuffer tmp = getKeyFor(this.current);
            this.key.reset(tmp.array(), 0, getLengthForKey(this.current));
            return this.key;
        }

       public ByteBuffer getValueFor(IntegerPair index) throws IOException {
            final IntUPairHadoopCLOutputMapperBuffer buffer = buffers[index.buffer];
            this.valueBytes = resizeByteBuffer(this.valueBytes, 20);
            this.valueBytes.position(0);
            this.valueBytes.putInt(   buffer.outputValIds[index.index]);
            this.valueBytes.putDouble(buffer.outputVals1[index.index]);
            this.valueBytes.putDouble(buffer.outputVals2[index.index]);
            return this.valueBytes;
        }

        public int getLengthForValue(IntegerPair index) {
            return 20;
        }

        @Override
        public final DataInputBuffer getValue() throws IOException {
            final ByteBuffer tmp = getValueFor(this.current);
            this.value.reset(tmp.array(), 0, getLengthForValue(this.current));
            return this.value;
        }

        @Override
        public HadoopCLDataInput getBulkReader() {
            return new HadoopCLBulkMapperReader() {
                @Override
                public final boolean hasMore() {
                    return !sortedIndices.isEmpty();
                }
                @Override
                public final void nextKey() throws IOException {
                    if (this.current != null) {
                        processed.push(this.current);
                    }
                    this.current = sortedIndices.pollFirst();
                    this.currentBuffer = getKeyFor(this.current);
                    this.currentBufferPosition = 0;
                }
                @Override
                public final void nextValue() throws IOException {
                    this.currentBuffer = getValueFor(this.current);
                    this.currentBufferPosition = 0;
                }
                @Override
                public final void prev() {
                    sortedIndices.add(this.current);
                    this.current = processed.pop();
                }
                @Override
                public final void readFully(int[] b, int off, int len) {
                    this.currentBuffer.position(this.currentBufferPosition);
                    this.currentBufferPosition += (len * 4);
                    this.currentBuffer.asIntBuffer().get(b, off, len);
                }
                @Override
                public final void readFully(double[] b, int off, int len) {
                    this.currentBuffer.position(this.currentBufferPosition);
                    this.currentBufferPosition += (len * 8);
                    this.currentBuffer.asDoubleBuffer().get(b, off, len);
                }
                @Override
                public final int readInt() {
                    this.currentBuffer.position(this.currentBufferPosition);
                    this.currentBufferPosition += 4;
                    return this.currentBuffer.getInt();
                }
            };
        }

    }

    @Override
    public HadoopCLKeyValueIterator getKeyValueIterator(Deque<OutputBufferSoFar> toWrite, int numReduceTasks) {
        return new KeyValueIterator(toWrite, numReduceTasks, this);
    }

}

