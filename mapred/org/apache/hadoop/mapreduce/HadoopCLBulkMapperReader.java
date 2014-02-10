package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.TreeSet;
import java.util.Stack;
import java.nio.ByteBuffer;
import org.apache.hadoop.mapreduce.HadoopCLKeyValueIterator.IntegerPair;

public abstract class HadoopCLBulkMapperReader implements HadoopCLDataInput {
    // private final HadoopCLKeyValueIterator iter;
    protected IntegerPair current = null;
    protected final Stack<IntegerPair> processed = new Stack<IntegerPair>();
    protected ByteBuffer currentBuffer;
    protected int currentBufferPosition;
    // private final TreeSet<Integer> sortedIndices;

    // public HadoopCLBulkMapperReader(HadoopCLKeyValueIterator iter) {
    //     this.iter = iter;
    //     this.sortedIndices = iter.sortedIndices;
    //     this.processed = new Stack<Integer>();
    // }

    /*
    @Override
    public boolean hasMore() {
        return !this.sortedIndices.isEmpty();
    }

    @Override
    public void nextKey() throws IOException {
        if (this.current != -1) {
            this.processed.push(this.current);
        }
        this.current = this.sortedIndices.pollFirst();
        this.currentBuffer = iter.getKeyFor(this.current);
        this.currentBufferPosition = 0;
    }

    @Override
    public void nextValue() throws IOException {
        this.currentBuffer = iter.getValueFor(this.current);
        this.currentBufferPosition = 0;
    }

    @Override
    public void prev() {
        this.sortedIndices.add(this.current);
        this.current = this.processed.pop();
    }

    @Override
    public void readFully(int[] b, int off, int len) {
        this.currentBuffer.position(this.currentBufferPosition);
        this.currentBufferPosition += (len * 4);
        this.currentBuffer.asIntBuffer().get(b, off, len);
    }

    @Override
    public void readFully(double[] b, int off, int len) {
        this.currentBuffer.position(this.currentBufferPosition);
        this.currentBufferPosition += (len * 8);
        this.currentBuffer.asDoubleBuffer().get(b, off, len);
    }

    @Override
    public int readInt() {
        this.currentBuffer.position(this.currentBufferPosition);
        this.currentBufferPosition += 4;
        return this.currentBuffer.getInt();
    }
    */

    @Override
    public boolean readBoolean() {
        throw new UnsupportedOperationException();
    }
    @Override
    public byte readByte() {
        throw new UnsupportedOperationException();
    }
    @Override
    public char readChar() {
        throw new UnsupportedOperationException();
    }
    @Override
    public double readDouble() {
        throw new UnsupportedOperationException();
    }
    @Override
    public float readFloat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(byte[] b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(float[] b, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }
    @Override
    public long readLong() {
        throw new UnsupportedOperationException();
    }
    @Override
    public short readShort() {
        throw new UnsupportedOperationException();
    }
    @Override
    public int readUnsignedShort() {
        throw new UnsupportedOperationException();
    }
    @Override
    public String readUTF() {
        throw new UnsupportedOperationException();
    }
    @Override
    public int skipBytes(int n) {
        throw new UnsupportedOperationException();
    }
    @Override
    public int readUnsignedByte() {
        throw new UnsupportedOperationException();
    }
}

