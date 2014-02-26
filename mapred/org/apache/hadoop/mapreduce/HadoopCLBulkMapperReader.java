package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.TreeSet;
import java.util.Stack;
import java.nio.ByteBuffer;
import org.apache.hadoop.mapreduce.HadoopCLKeyValueIterator.IntegerPair;

public abstract class HadoopCLBulkMapperReader implements HadoopCLDataInput {
    protected IntegerPair current = null;
    protected ByteBuffer currentBuffer;
    protected int currentBufferPosition;

    @Override
    public void reset() {
        this.currentBufferPosition = 0;
    }

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

