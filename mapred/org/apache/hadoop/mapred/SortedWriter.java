package org.apache.hadoop.mapred;

import org.apache.hadoop.io.BSparseVectorWritable;
import java.util.HashMap;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import java.util.HashMap;
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

/*
 * The Hadoop IFile format is confusing and difficult to parse from the code so
 * I'm documenting it here. An IFile may be used for several purposes, though
 * the only one I have experience with is as an on-disk storage for the
 * intermediate results of a map task before they are passed to a reduce task.
 *
 * An IFile is divided into chunks of kv-pairs, where each chunk is sorted by
 * key and contains kv-pairs which belong to the same partition (by default
 * partition is determined by the HashPartitioner class). You can think of each
 * of these chunks as a list of serialized kv-pairs with some extra metadata
 * added to help with finding where the kv-pairs start and end.
 *
 * At a high level, each chunk contains an array of binary kv-pairs followed by
 * a trailing EOF marker and a checksum. The checksum is the last 4 bytes and
 * is calculated by calls to the DataChecksum class from the IFileOutputStream,
 * but actually written in a call to IFileOutputStream.finish(). The EOF marker
 * is composed of two IFile.EOF_MARKER objects and has a length determined by
 * writeVInt. The array of kv-pairs is composed of cells, one cell for each
 * pair.
 *
 * Each cell has the following format:
 *   a variable length integer for the key length
 *   a variable length integer for the value length
 *   a binary chunk containing both the serialized key and value
 * It is possible to find the key and value in the binary chunk using the
 * lengths stored with them.
 *
 * For fast lookup of a certain partition's offset in an IFile, Hadoop also uses
 * the SpillRecord object (which is essentially a list of IndexRecord objects,
 * one for each partition in the IFile). Each IndexRecord object stores the
 * offset in the IFile that the partition starts at, the decompressed size of
 * that chunk in bytes excluding the checksum, and the compressed size of that
 * chunk in bytes including the checksum (if compression is disabled, that just
 * means the compressed size = decompressed size + 4 bytes). These spill records
 * may be stored in memory or (if there are many of them) dumped to an on-disk
 * file and stored as longs.
 */

public class SortedWriter<K extends Comparable<K> & Writable, V extends Comparable<V> & Writable> extends
        IFile.Writer<K, V> implements IndexedSortable {

    private final int spillNo;
    private final boolean isHacky;
    private final OutputBuffer outputBuffer;
    private final RawComparator<K> keyComparator;
    private final List<Integer> recordMarks;
    private final List<Integer> valueMarks;
    private final List<Integer> endOfRecords;
    private final List<Integer> keyPartitions;
    private final boolean multiPartition;

    private HashMap<Integer, Long> partitionSegmentStarts = null;
    private HashMap<Integer, Long> partitionRawLengths = null;
    private HashMap<Integer, Long> partitionCompressedLengths = null;

    private final int partitions;
    private final org.apache.hadoop.mapreduce.Partitioner partitioner;
    private final long startPos;

    public SortedWriter(Configuration conf, FileSystem fs, Path file, 
                  Class<K> keyClass, Class<V> valueClass,
                  CompressionCodec codec,
                  Counters.Counter writesCounter,
                  RawComparator<K> keyComparator, boolean isHacky,
                  int spillNo, boolean multiPartition) throws IOException {
      super(conf, fs, file, keyClass, valueClass, codec, writesCounter);
      this.outputBuffer = new OutputBuffer();
      this.keyComparator = keyComparator;
      this.recordMarks = new ArrayList<Integer>();
      this.valueMarks = new ArrayList<Integer>();
      this.endOfRecords = new ArrayList<Integer>();
      this.keyPartitions = new ArrayList<Integer>();
      this.partitions = conf.getInt("mapred.reduce.tasks", 1);
      this.isHacky = isHacky;
      this.spillNo = spillNo;
      this.startPos = this.rawOut.getPos();
      this.multiPartition = multiPartition;

      if (this.partitions > 0) {
        partitioner = (org.apache.hadoop.mapreduce.Partitioner)
          ReflectionUtils.newInstance(conf.getClass("mapred.partitioner.class",
                    HashPartitioner.class,
                    org.apache.hadoop.mapreduce.Partitioner.class), conf);
      } else {
        partitioner = new org.apache.hadoop.mapreduce.Partitioner() {
          @Override
          public int getPartition(Object key, Object value, int numPartitions) {
            return -1;
          }
        };
      }
    }
    
    public SortedWriter(Configuration conf, FSDataOutputStream out, 
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec, Counters.Counter writesCounter,
        RawComparator<K> keyComparator, boolean isHacky,
        int spillNo, boolean multiPartition) throws IOException {
      super(conf, out, keyClass, valueClass, codec, writesCounter);
      this.outputBuffer = new OutputBuffer();
      this.keyComparator = keyComparator;
      this.recordMarks = new ArrayList<Integer>();
      this.valueMarks = new ArrayList<Integer>();
      this.endOfRecords = new ArrayList<Integer>();
      this.keyPartitions = new ArrayList<Integer>();
      this.partitions = conf.getInt("mapred.reduce.tasks", 1);
      this.isHacky = isHacky;
      this.spillNo = spillNo;
      this.startPos = this.rawOut.getPos();
      this.multiPartition = multiPartition;

      if (this.partitions > 0) {
        partitioner = (org.apache.hadoop.mapreduce.Partitioner)
          ReflectionUtils.newInstance(conf.getClass("mapred.partitioner.class",
                    HashPartitioner.class,
                    org.apache.hadoop.mapreduce.Partitioner.class), conf);
      } else {
        partitioner = new org.apache.hadoop.mapreduce.Partitioner() {
          @Override
          public int getPartition(Object key, Object value, int numPartitions) {
            return -1;
          }
        };
      }
    }

    public int compare(int i, int j) {
        int iPart = this.keyPartitions.get(i);
        int jPart = this.keyPartitions.get(j);
        if (iPart != jPart) return iPart - jPart;

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
        Integer tmpPartition = this.keyPartitions.get(i);

        this.recordMarks.set(i, this.recordMarks.get(j));
        this.valueMarks.set(i, this.valueMarks.get(j));
        this.endOfRecords.set(i, this.endOfRecords.get(j));
        this.keyPartitions.set(i, this.keyPartitions.get(j));

        this.recordMarks.set(j, tmpRecordMark);
        this.valueMarks.set(j, tmpValueMark);
        this.endOfRecords.set(j, tmpEndOfRecord);
        this.keyPartitions.set(j, tmpPartition);
    }

    public HashMap<Integer, Long> getPartitionSegmentStarts() {
        return partitionSegmentStarts;
    }

    public HashMap<Integer, Long> getPartitionRawLengths() {
        return partitionRawLengths;
    }

    public HashMap<Integer, Long> getPartitionCompressedLengths() {
        return partitionCompressedLengths;
    }

    private void finishOffPartition(int currentPartition) throws IOException {
        WritableUtils.writeVInt(out, IFile.EOF_MARKER);
        WritableUtils.writeVInt(out, IFile.EOF_MARKER);
        decompressedBytesWritten +=
            2 * WritableUtils.getVIntSize(IFile.EOF_MARKER);
        out.flush();

        compressedBytesWritten = rawOut.getPos() -
            this.partitionSegmentStarts.get(currentPartition);
        if (compressOutput) {
            compressedOut.finish();
            compressedOut.resetState();
        }
        checksumOut.finish();
        compressedBytesWritten = this.rawOut.getPos() -
            this.partitionSegmentStarts.get(currentPartition);

        this.partitionRawLengths.put(currentPartition,
            decompressedBytesWritten);
        this.partitionCompressedLengths.put(currentPartition,
            compressedBytesWritten);

        checksumOut = new IFileOutputStream(rawOut);
        this.out = new FSDataOutputStream(checksumOut,null);

        this.partitionSegmentStarts.put(currentPartition+1, this.rawOut.getPos());

        decompressedBytesWritten = 0;
    }

    public void close() throws IOException {

      partitionSegmentStarts = new HashMap<Integer, Long>();
      partitionRawLengths = new HashMap<Integer, Long>();
      partitionCompressedLengths = new HashMap<Integer, Long>();

      long localStart = this.rawOut.getPos();

      if (this.recordMarks.isEmpty()) {
          StringBuilder sb = new StringBuilder();
          sb.append("Empty recordMarks\n");
          StackTraceElement[] trace = Thread.currentThread().getStackTrace();
          for (StackTraceElement t : trace) {
              sb.append("  "+t.toString()+"\n");
          }
          System.err.println(sb.toString());

          if (multiPartition) {
              currentPartition = 0;
              this.partitionSegmentStarts.put(currentPartition,
                  this.rawOut.getPos());
              while (currentPartition < partitions - 1) {
                  finishOffPartition(currentPartition);
                  currentPartition++;
              }
          }
      } else {

          if (multiPartition) {
              currentPartition = 0;
          } else {
              currentPartition = this.keyPartitions.get(0);
          }
          this.partitionSegmentStarts.put(currentPartition,
              this.rawOut.getPos());

          // Do sorted writes
          for (int i = 0; i < this.recordMarks.size(); i++) {
              int part = this.keyPartitions.get(i);
              int startRecord = this.recordMarks.get(i);
              int startVal = this.valueMarks.get(i);
              int endRecord = this.endOfRecords.get(i);

              if (part != currentPartition) {
                // Should only be possible when dumping from BufferRunner
                System.err.println("Found different partition");
                StackTraceElement[] trace = Thread.currentThread().getStackTrace();
                for (StackTraceElement t : trace) {
                  System.err.println("  "+t.toString());
                }

                while (currentPartition != part) {
                  finishOffPartition(currentPartition);
                  currentPartition++;
                }
              }

              WritableUtils.writeVInt(out, startVal - startRecord); // keyLength
              WritableUtils.writeVInt(out, endRecord - startVal); // valueLength
              this.outputBuffer.dump(out, startRecord, endRecord);
              decompressedBytesWritten += (endRecord - startRecord) + 
                     WritableUtils.getVIntSize(startVal - startRecord) + 
                     WritableUtils.getVIntSize(endRecord - startVal);
          }
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

      if (this.recordMarks.isEmpty()) {
          compressedBytesWritten = rawOut.getPos() - localStart;
      } else {
          compressedBytesWritten = rawOut.getPos() -
              this.partitionSegmentStarts.get(currentPartition);
      }
  
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

      if (this.recordMarks.isEmpty()) {
          compressedBytesWritten = rawOut.getPos() - localStart;

      } else {
          compressedBytesWritten = rawOut.getPos() -
            this.partitionSegmentStarts.get(currentPartition);

          this.partitionRawLengths.put(currentPartition,
              decompressedBytesWritten);
          this.partitionCompressedLengths.put(currentPartition,
              compressedBytesWritten);
      }

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
      int part = this.partitioner.getPartition(key, value,
            this.partitions);
      keyPartitions.add(part);
      ++numRecordsWritten;
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
                int left = len - avail;
                int sofar = avail;
                while (left > 0) {
                    addNewBuffer();
                    avail = availableInCurrentBuffer();
                    int towrite = avail;
                    if (left < towrite) towrite = left;
                    currentBuffer.buffer.put(b, off + sofar, towrite);
                    sofar += towrite;
                    left -= towrite;
                }
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
