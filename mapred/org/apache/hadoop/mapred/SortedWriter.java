package org.apache.hadoop.mapred;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.RawComparator;
import java.nio.ByteBuffer;
import java.io.DataOutput;

public class SortedWriter<K extends Comparable<K> & Writable, V extends Comparable<V> & Writable> extends
        IFile.Writer<K, V> implements IndexedSortable {

    private final OutputBuffer outputBuffer;
    private final RawComparator<K> keyComparator;
    private final List<Integer> recordMarks;
    private final List<Integer> valueMarks;
    private final List<Integer> endOfRecords;

    public SortedWriter(Configuration conf, FileSystem fs, Path file, 
                  Class<K> keyClass, Class<V> valueClass,
                  CompressionCodec codec,
                  Counters.Counter writesCounter,
                  RawComparator<K> keyComparator) throws IOException {
      super(conf, fs, file, keyClass, valueClass, codec, writesCounter);
      this.outputBuffer = new OutputBuffer();
      this.keyComparator = keyComparator;
      this.recordMarks = new ArrayList<Integer>();
      this.valueMarks = new ArrayList<Integer>();
      this.endOfRecords = new ArrayList<Integer>();
    }
    
    public SortedWriter(Configuration conf, FSDataOutputStream out, 
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec, Counters.Counter writesCounter,
        RawComparator<K> keyComparator) throws IOException {
      super(conf, out, keyClass, valueClass, codec, writesCounter);
      this.outputBuffer = new OutputBuffer();
      this.keyComparator = keyComparator;
      this.recordMarks = new ArrayList<Integer>();
      this.valueMarks = new ArrayList<Integer>();
      this.endOfRecords = new ArrayList<Integer>();
    }

    public int compare(int i, int j) {
        int iOffset = this.recordMarks.get(i);
        int iLength = this.valueMarks.get(i) - iOffset;
        int jOffset = this.recordMarks.get(j);
        int jLength = this.valueMarks.get(j) - jOffset;
        return this.outputBuffer.compare(iOffset, iLength, jOffset, jLength,
            this.keyComparator);
    }

    public void swap(int i, int j) {
        Integer tmpRecordMark = this.recordMarks.get(i);
        Integer tmpValueMark = this.valueMarks.get(i);
        Integer tmpEndOfRecord = this.endOfRecords.get(i);

        this.recordMarks.set(i, this.recordMarks.get(j));
        this.valueMarks.set(i, this.valueMarks.get(j));
        this.endOfRecords.set(i, this.endOfRecords.get(j));

        this.recordMarks.set(j, tmpRecordMark);
        this.valueMarks.set(j, tmpValueMark);
        this.endOfRecords.set(j, tmpEndOfRecord);
    }

    public void close() throws IOException {

      if (this.recordMarks.isEmpty()) return;

      new QuickSort().sort(this, 0, this.recordMarks.size());

      // Do sorted writes
      for (int i = 0; i < this.recordMarks.size(); i++) {
          int startRecord = this.recordMarks.get(i);
          int startVal = this.valueMarks.get(i);
          int endRecord = this.endOfRecords.get(i);
          WritableUtils.writeVInt(out, startVal - startRecord); // keyLength
          WritableUtils.writeVInt(out, endRecord - startVal); // valueLength
          this.outputBuffer.dump(out, startRecord, endRecord);
      }

      // Close the serializers
      keySerializer.close();
      valueSerializer.close();

      // Write EOF_MARKER for key/value length
      WritableUtils.writeVInt(out, IFile.EOF_MARKER);
      WritableUtils.writeVInt(out, IFile.EOF_MARKER);
      decompressedBytesWritten += 2 * WritableUtils.getVIntSize(IFile.EOF_MARKER);
      
      //Flush the stream
      out.flush();
  
      if (compressOutput) {
        // Flush
        compressedOut.finish();
        compressedOut.resetState();
      }
      
      // Close the underlying stream iff we own it...
      if (ownOutputStream) {
        out.close();
      }
      else {
        // Write the checksum
        checksumOut.finish();
      }

      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // Return back the compressor
        CodecPool.returnCompressor(compressor);
        compressor = null;
      }

      out = null;
      if(writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
    }

    public void append(K key, V value) throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+ key.getClass()
                              +" is not "+ keyClass);
      if (value.getClass() != valueClass)
        throw new IOException("wrong value class: "+ value.getClass()
                              +" is not "+ valueClass);

      int keyStart = outputBuffer.currentOffset();
      recordMarks.add(keyStart);
      key.write(outputBuffer);
      int valueStart = outputBuffer.currentOffset();
      valueMarks.add(valueStart);
      value.write(outputBuffer);
      int recordEnd = outputBuffer.currentOffset();
      endOfRecords.add(recordEnd);
      ++numRecordsWritten;
      decompressedBytesWritten += recordEnd - keyStart + 
                                 WritableUtils.getVIntSize(valueStart - keyStart) + 
                                 WritableUtils.getVIntSize(recordEnd - valueStart);
    }
    
    public void append(DataInputBuffer key, DataInputBuffer value)
        throws IOException {
      throw new UnsupportedOperationException();
    }
    
    public long getRawLength() {
      return decompressedBytesWritten;
    }
    
    public long getCompressedLength() {
      return compressedBytesWritten;
    }


    private static class Buffer {
        public final ByteBuffer buffer;
        private final int capacity;
        private final int offset;
        private Buffer nextBuffer;

        public Buffer(int bufferSize, int offset) {
            this.buffer = ByteBuffer.allocate(bufferSize);
            this.capacity = bufferSize;
            this.offset = offset;
        }

        public void setNextBuffer(Buffer next) {
            if (this.nextBuffer != null) {
                throw new RuntimeException("Setting next buffer multiple times");
            }
            this.nextBuffer = next;
        }

        public Buffer getNextBuffer() {
            return this.nextBuffer;
        }

        public int available() {
            return buffer.remaining();
        }

        public int offset() {
            return this.offset;
        }

        public int filled() {
            return buffer.position();
        }
    }

    private static class OutputBuffer implements DataOutput {
        private final List<Buffer> buffers;
        private static final int bufferSize = 5242880;
        private int soFar;
        private Buffer currentBuffer;

        private byte[] iArr = new byte[4];
        private byte[] jArr = new byte[4];

        public OutputBuffer() {
            this.buffers = new LinkedList<Buffer>();
            this.soFar = 0;
            Buffer b = new Buffer(bufferSize, 0);
            this.buffers.add(b);
            this.currentBuffer = b;
        }

        private void addNewBuffer() {
            Buffer b = new Buffer(bufferSize, soFar + currentBuffer.filled());
            soFar += currentBuffer.filled();
            currentBuffer.setNextBuffer(b);
            buffers.add(b);
            currentBuffer = b;
        }

        private int availableInCurrentBuffer() {
            return currentBuffer.available();
        }

        public int currentOffset() {
            return currentBuffer.offset() + currentBuffer.filled();
        }

        private Buffer findBufferContainingOffset(int offset) {
            Iterator<Buffer> iter = buffers.iterator();
            while (iter.hasNext()) {
                Buffer b = iter.next();
                if (b.offset() <= offset && b.offset() + b.filled() > offset) {
                    return b;
                }
            }
            return null;
        }

        private void transfer(int globalOffset, int length, byte[] arr) {
            int soFar = 0;
            int currentGlobalOffset = globalOffset;
            Buffer buf = findBufferContainingOffset(currentGlobalOffset);
            while (soFar < length) {
                int toTransfer = length - soFar;
                int inBuf = buf.offset() + buf.filled() - currentGlobalOffset;
                if (inBuf < toTransfer) {
                    toTransfer = inBuf;
                }

                System.arraycopy(buf.buffer.array(), currentGlobalOffset - buf.offset(),
                    arr, soFar, toTransfer);
                soFar += toTransfer;
                currentGlobalOffset += toTransfer;
                buf = buf.getNextBuffer();
            }
        }

        public void dump(FSDataOutputStream out, int startOffset, int endOffset) throws IOException {
            int currentGlobalOffset = startOffset;
            Buffer buf = findBufferContainingOffset(currentGlobalOffset);
            while (currentGlobalOffset < endOffset) {
                int toTransfer = endOffset - currentGlobalOffset;
                int inBuf = buf.offset() + buf.filled() - currentGlobalOffset;
                if (inBuf < toTransfer) {
                    toTransfer = inBuf;
                }
                out.write(buf.buffer.array(), currentGlobalOffset - buf.offset(), toTransfer);
                currentGlobalOffset += toTransfer;
                buf = buf.getNextBuffer();
            }
        }

        public int compare(int iOffset, int iLength, int jOffset, int jLength,
                RawComparator comparator) {

            if (iLength > iArr.length) {
                iArr = new byte[iLength];
            }
            if (jLength > jArr.length) {
                jArr = new byte[jLength];
            }

            transfer(iOffset, iLength, iArr);
            transfer(jOffset, jLength, jArr);

            return comparator.compare(iArr, 0, iLength, jArr, 0, jLength);
        }

        @Override
        public void write(byte[] b) {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            int avail = availableInCurrentBuffer();
            if (avail >= len) {
                currentBuffer.buffer.put(b, off, len);
            } else {
                currentBuffer.buffer.put(b, off, avail);
                addNewBuffer();
                currentBuffer.buffer.put(b, off + avail, len - avail);
            }
        }

        @Override
        public void write(int b) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeBoolean(boolean v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeByte(int v) {
            if (availableInCurrentBuffer() < 1) {
                addNewBuffer();
            }
            currentBuffer.buffer.put((byte)(v & 0xff));
        }

        @Override
        public void writeBytes(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeChar(int v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeChars(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeDouble(double v) {
            if (availableInCurrentBuffer() < 8) {
                addNewBuffer();
            }
            currentBuffer.buffer.putDouble(v);
        }

        @Override
        public void writeFloat(float v) {
            if (availableInCurrentBuffer() < 4) {
                addNewBuffer();
            }
            currentBuffer.buffer.putFloat(v);
        }

        @Override
        public void writeInt(int v) {
            if (availableInCurrentBuffer() < 4) {
                addNewBuffer();
            }
            currentBuffer.buffer.putInt(v);
        }

        @Override
        public void writeLong(long v) {
            if (availableInCurrentBuffer() < 8) {
                addNewBuffer();
            }
            currentBuffer.buffer.putLong(v);
        }

        @Override
        public void writeShort(int v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeUTF(String s) {
            throw new UnsupportedOperationException();
        }
    }


}
