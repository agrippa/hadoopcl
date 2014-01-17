package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import org.apache.hadoop.mapred.MapTask.MapOutputBuffer;
import java.nio.ByteBuffer;

public class HadoopCLBulkCombinerReader implements HadoopCLDataInput {
    private final int size;
    private final int end;
    private int current;
    private int currentBase;
    private int currentOffset;
    private int bufvoid;
    
    private final int[] kvoffsets;
    private final int[] kvindices;
    private final byte[] kvbuffer;
    private final ByteBuffer bb;

    public HadoopCLBulkCombinerReader(int start, int end, int[] kvoffsets,
        int[] kvindices, byte[] kvbuffer, int bufvoid) {
      this.current = start - 1;
      this.end = end;
      this.size = end - start;
      this.kvoffsets = kvoffsets;
      this.kvindices = kvindices;
      this.kvbuffer = kvbuffer;
      bb = ByteBuffer.wrap(kvbuffer);
      this.currentBase = -1;
      this.currentOffset = -1;
      this.bufvoid = bufvoid;
    }

    public boolean hasMore() {
      int newCurr = current + 1;
      if (newCurr < end) {
          this.currentBase = kvindices[kvoffsets[newCurr % kvoffsets.length]
              + MapOutputBuffer.KEYSTART];
          this.currentOffset = 0;
          this.current = newCurr;
          return true;
      } else {
          return false;
      }
    }

    private void repositionBuffer() {
        bb.position((this.currentBase + this.currentOffset) % bufvoid);
    }

    private int currentAbsolutePosition() {
        return (this.currentBase + this.currentOffset) % bufvoid;
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
        repositionBuffer();
        this.currentOffset += 2;
        return bb.getChar();
    }
    @Override
    public double readDouble() {
        repositionBuffer();
        this.currentOffset += 8;
        return bb.getDouble();
    }
    @Override
    public float readFloat() {
        repositionBuffer();
        this.currentOffset += 4;
        return bb.getFloat();
    }

    @Override
    public void readFully(byte[] b) {
        throw new UnsupportedOperationException();
    }
    @Override
    public void readFully(byte[] b, int off, int len) {
        repositionBuffer();
        if (currentAbsolutePosition() + len > bufvoid) {
            int distToEnd = bufvoid - currentAbsolutePosition();
            bb.get(b, off, distToEnd);
            bb.get(b, off + distToEnd, len - distToEnd);
        } else {
            bb.get(b, off, len);
        }
        this.currentOffset += len;
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
        repositionBuffer();
        this.currentOffset += 8;
        return bb.getLong();
    }
    @Override
    public short readShort() {
        repositionBuffer();
        this.currentOffset += 2;
        return bb.getShort();
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
