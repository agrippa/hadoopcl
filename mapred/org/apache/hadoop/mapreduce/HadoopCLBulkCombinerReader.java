package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.io.DataInput;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.DoubleBuffer;

public class HadoopCLBulkCombinerReader implements HadoopCLDataInput {
    private final int end;
    private int current;
    private int currentBase;
    private int currentOffset;
    private int bufvoid;
    
    private final int[] kvoffsets;
    private final int[] kvindices;
    private final byte[] kvbuffer;
    private final ByteBuffer bb;
    private IntBuffer intBuffer;
    private DoubleBuffer doubleBuffer;
    private final IntBuffer absoluteIntBuffer;
    private final DoubleBuffer absoluteDoubleBuffer;

    public HadoopCLBulkCombinerReader(int start, int end, int[] kvoffsets,
        int[] kvindices, byte[] kvbuffer, int bufvoid) {
      this.current = start - 1;
      this.end = end;
      this.kvoffsets = kvoffsets;
      this.kvindices = kvindices;
      this.kvbuffer = kvbuffer;
      this.bb = ByteBuffer.wrap(kvbuffer);
      this.intBuffer = this.bb.asIntBuffer();
      this.doubleBuffer = this.bb.asDoubleBuffer();
      this.absoluteIntBuffer = this.bb.asIntBuffer();
      this.absoluteDoubleBuffer = this.bb.asDoubleBuffer();
      this.currentBase = -1;
      this.currentOffset = -1;
      this.bufvoid = bufvoid;
    }

    @Override
    public int compareKeys(HadoopCLDataInput other) throws IOException {
        return -1 * other.compareKeys(this);
    }

    @Override
    public boolean hasMore() {
        return current + 1 < end;
    }

    @Override
    public void nextKey() throws IOException {
        int newCurr = current + 1;
        if (newCurr < end) {
            final int kvoff = kvoffsets[newCurr % kvoffsets.length];
            this.currentBase = kvindices[kvoff + MapOutputBuffer.KEYSTART];
            this.currentOffset = 0;
        }
        this.current = newCurr;
    }

    @Override
    public void nextValue() throws IOException {
        if (this.current < end) {
            final int kvoff = kvoffsets[this.current % kvoffsets.length];
            this.currentBase = kvindices[kvoff + MapOutputBuffer.VALSTART];
            this.currentOffset = 0;
        }
    }

    @Override
    public void prev() {
        int previous = current - 1;
        if (previous >= 0) {
            final int kvoff = kvoffsets[previous % kvoffsets.length];
            this.currentBase = kvindices[kvoff + MapOutputBuffer.KEYSTART];
            this.currentOffset = 0;
        }
        this.current = previous;
    }

    @Override
    public void reset() {
        this.currentOffset = 0;
    }

    private int currentAbsolutePosition() {
        return (this.currentBase + this.currentOffset) % bufvoid;
    }

    private void repositionBuffer() {
        final int pos = currentAbsolutePosition();
        bb.position(pos);
    }

    private IntBuffer repositionIntBuffer() {
        final int pos = currentAbsolutePosition();
        if (pos % 4 == 0) {
            absoluteIntBuffer.position(pos / 4);
            return absoluteIntBuffer;
        } else {
            repositionBuffer();
            intBuffer = bb.asIntBuffer();
            return intBuffer;
        }
    }

    private DoubleBuffer repositionDoubleBuffer() {
        final int pos = currentAbsolutePosition();
        if (pos % 8 == 0) {
            absoluteDoubleBuffer.position(pos / 8);
            return absoluteDoubleBuffer;
        } else {
            repositionBuffer();
            doubleBuffer = bb.asDoubleBuffer();
            return doubleBuffer;
        }
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
        // repositionBuffer();
        // this.currentOffset += 2;
        // return bb.getChar();
    }
    @Override
    public double readDouble() {
        throw new UnsupportedOperationException();
        // repositionBuffer();
        // this.currentOffset += 8;
        // return bb.getDouble();
    }
    @Override
    public float readFloat() {
        throw new UnsupportedOperationException();
        // repositionBuffer();
        // this.currentOffset += 4;
        // return bb.getFloat();
    }

    @Override
    public void readFully(byte[] b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        throw new UnsupportedOperationException();
        // repositionBuffer();
        // if (currentAbsolutePosition() + len > bufvoid) {
        //     int distToEnd = bufvoid - currentAbsolutePosition();
        //     bb.get(b, off, distToEnd);
        //     this.currentOffset += distToEnd;
        //     repositionBuffer();
        //     bb.get(b, off + distToEnd, len - distToEnd);
        //     this.currentOffset += len - distToEnd;
        // } else {
        //     bb.get(b, off, len);
        //     this.currentOffset += len;
        // }
    }

    @Override
    public void readFully(int[] b, int off, int len) {
        final int lenInBytes = len * 4;
        final int distToEnd = bufvoid - currentAbsolutePosition();

        if (distToEnd < lenInBytes) {
            byte[] aggregate = new byte[lenInBytes];
            System.arraycopy(kvbuffer, currentAbsolutePosition(),
                aggregate, 0, distToEnd);
            this.currentOffset += distToEnd;
            System.arraycopy(kvbuffer, currentAbsolutePosition(),
                aggregate, distToEnd, lenInBytes - distToEnd);
            this.currentOffset += (lenInBytes - distToEnd);

            ByteBuffer.wrap(aggregate).asIntBuffer().get(b, off, len);
        } else {
            repositionIntBuffer().get(b, off, len);
            this.currentOffset += lenInBytes;
        }
    }

    @Override
    public void readFully(double[] b, int off, int len) {
        final int lenInBytes = len * 8;
        final int distToEnd = bufvoid - currentAbsolutePosition();

        if (distToEnd < lenInBytes) {
            byte[] aggregate = new byte[lenInBytes];
            System.arraycopy(kvbuffer, currentAbsolutePosition(),
                aggregate, 0, distToEnd);
            this.currentOffset += distToEnd;
            System.arraycopy(kvbuffer, currentAbsolutePosition(),
                aggregate, distToEnd, lenInBytes - distToEnd);
            this.currentOffset += (lenInBytes - distToEnd);

            ByteBuffer.wrap(aggregate).asDoubleBuffer().get(b, off, len);
        } else {
            repositionDoubleBuffer().get(b, off, len);
            this.currentOffset += lenInBytes;
        }
    }

    @Override
    public void readFully(float[] b, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readInt() {
        repositionBuffer();
        this.currentOffset += 4;
        return bb.getInt();
    }
    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }
    @Override
    public long readLong() {
        throw new UnsupportedOperationException();
        // repositionBuffer();
        // this.currentOffset += 8;
        // return bb.getLong();
    }
    @Override
    public short readShort() {
        throw new UnsupportedOperationException();
        // repositionBuffer();
        // this.currentOffset += 2;
        // return bb.getShort();
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
