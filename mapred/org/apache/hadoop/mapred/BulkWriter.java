package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.HadoopCLPartitioner;
import org.apache.hadoop.io.IntWritable;
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
import java.io.DataInput;
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

public class BulkWriter<K extends Comparable<K> & Writable, V extends Comparable<V> & Writable> extends
        IFile.Writer<K, V> {

    private HashMap<Integer, Long> partitionSegmentStarts = new HashMap<Integer, Long>();
    private HashMap<Integer, Long> partitionRawLengths = new HashMap<Integer, Long>();
    private HashMap<Integer, Long> partitionCompressedLengths = new HashMap<Integer, Long>();

    private final int partitions;
    private final org.apache.hadoop.mapreduce.Partitioner partitioner;
    private final HadoopCLPartitioner appendPartitioner;
    private final long startPos;
    private int currentPartition = 0;

    public BulkWriter(Configuration conf, FileSystem fs, Path file, 
                  Class<K> keyClass, Class<V> valueClass,
                  CompressionCodec codec,
                  Counters.Counter writesCounter,
                  HadoopCLPartitioner appendPartitioner) throws IOException {
      super(conf, fs, file, keyClass, valueClass, codec, writesCounter);
      this.partitions = conf.getInt("mapred.reduce.tasks", 1);
      this.startPos = this.rawOut.getPos();
      this.appendPartitioner = appendPartitioner;
      this.partitionSegmentStarts.put(0,
          this.rawOut.getPos());

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
    
    public BulkWriter(Configuration conf, FSDataOutputStream out, 
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec, Counters.Counter writesCounter,
        HadoopCLPartitioner appendPartitioner) throws IOException {
      super(conf, out, keyClass, valueClass, codec, writesCounter);
      this.partitions = conf.getInt("mapred.reduce.tasks", 1);
      this.startPos = this.rawOut.getPos();
      this.appendPartitioner = appendPartitioner;
      this.partitionSegmentStarts.put(0,
          this.rawOut.getPos());

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
      while (currentPartition < partitions) {
          finishOffPartition(currentPartition);
          currentPartition++;
      }

      // Don't care about compressed or decompressed bytes written for the
      // case of multiPartition writing
      
      // Close the serializers
      keySerializer.close();
      valueSerializer.close();

      // Write EOF_MARKER for key/value length
      WritableUtils.writeVInt(out, IFile.EOF_MARKER);
      WritableUtils.writeVInt(out, IFile.EOF_MARKER);

      out.flush();

      if (compressOutput) {
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

      // this.partitionRawLengths.put(partitions - 1,
      //     decompressedBytesWritten);
      // this.partitionCompressedLengths.put(partitions - 1,
      //     compressedBytesWritten);

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

      final int part = this.partitioner.getPartition(key, value,
            this.partitions);
      while (currentPartition != part) {
          finishOffPartition(currentPartition);
          currentPartition++;
      }

      keySerializer.serialize(key);
      final int keyLength = buffer.getLength();
      valueSerializer.serialize(value);
      final int valueLength = buffer.getLength() - keyLength;

      WritableUtils.writeVInt(out, keyLength);
      WritableUtils.writeVInt(out, valueLength);

      out.write(buffer.getData(), 0, buffer.getLength());
      buffer.reset();

      decompressedBytesWritten += keyLength + valueLength +
          WritableUtils.getVIntSize(keyLength) +
          WritableUtils.getVIntSize(valueLength);

      ++numRecordsWritten;
    }
    
    public void append(DataInputBuffer key, DataInputBuffer value)
        throws IOException {

      final int part = this.appendPartitioner.getPartitionOfCurrent(this.partitions);
      while (currentPartition != part) {
          finishOffPartition(currentPartition);
          currentPartition++;
      }

      int keyLength = key.getLength() - key.getPosition();
      int valueLength = value.getLength() - value.getPosition();

      WritableUtils.writeVInt(out, keyLength);
      WritableUtils.writeVInt(out, valueLength);

      out.write(key.getData(), key.getPosition(), keyLength);
      out.write(value.getData(), value.getPosition(), valueLength);

      decompressedBytesWritten += keyLength + valueLength +
          WritableUtils.getVIntSize(keyLength) +
          WritableUtils.getVIntSize(valueLength);

      ++numRecordsWritten;
    }
    
    public long getRawLength() {
      return decompressedBytesWritten;
    }
    
    public long getCompressedLength() {
      return compressedBytesWritten;
    }
}
