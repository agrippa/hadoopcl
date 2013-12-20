package org.apache.hadoop.mapred;

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
import org.apache.hadoop.io.IntWritable;

public class SortedWriter<K extends Comparable<K>, V extends Comparable<V>> extends
    IFile.Writer<K, V> /* implements IndexedSortable */ {

    class Buffered implements Comparable<Buffered>{
      public final K key;
      public final V val;
      public Buffered(K key, V val) { this.key = key; this.val = val; }

      @Override
      public int compareTo(Buffered other) {
        return this.key.compareTo(other.key);
      }
    }

    private final Set<Buffered> buffered = new TreeSet<Buffered>();
    // private final List<Buffered> buffered = new ArrayList<Buffered>();

    public SortedWriter(Configuration conf, FileSystem fs, Path file, 
                  Class<K> keyClass, Class<V> valueClass,
                  CompressionCodec codec,
                  Counters.Counter writesCounter) throws IOException {
      super(conf, fs, file, keyClass, valueClass, codec, writesCounter);
    }
    
    public SortedWriter(Configuration conf, FSDataOutputStream out, 
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec, Counters.Counter writesCounter)
        throws IOException {
      super(conf, out, keyClass, valueClass, codec, writesCounter);
    }

    // @Override
    // public int compare(int i, int j) {
    //   return buffered.get(i).key.compareTo(buffered.get(j).key);
    // }

    // @Override
    // public void swap(int i, int j) {
    //   Buffered tmp = this.buffered.get(i);
    //   this.buffered.set(i, this.buffered.get(j));
    //   this.buffered.set(j, tmp);
    // }

    public void close() throws IOException {

      // new QuickSort().sort(this, 0, buffered.size());

      // Do sorted writes
      for (Buffered b : this.buffered) {
        K key = b.key;
        V value = b.val;

        // System.out.print(((IntWritable)key).get()+" ");

        keySerializer.serialize(key);
        int keyLength = buffer.getLength();
        if (keyLength < 0) {
          throw new IOException("Negative key-length not allowed: " + keyLength + 
                                " for " + key);
        }

        valueSerializer.serialize(value);
        int valueLength = buffer.getLength() - keyLength;
        if (valueLength < 0) {
          throw new IOException("Negative value-length not allowed: " + 
                                valueLength + " for " + value);
        }
        WritableUtils.writeVInt(out, keyLength);
        WritableUtils.writeVInt(out, valueLength);
        out.write(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
        decompressedBytesWritten += keyLength + valueLength + 
                                   WritableUtils.getVIntSize(keyLength) + 
                                   WritableUtils.getVIntSize(valueLength);
      }
      // System.out.println();

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

      buffered.add(new Buffered(key, value));

      // StackTraceElement[] trace = Thread.currentThread().getStackTrace();
      // for (StackTraceElement el : trace) System.out.println(el.toString());
      // System.out.println();
      
      // // Append the 'key'
      // keySerializer.serialize(key);
      // int keyLength = buffer.getLength();
      // if (keyLength < 0) {
      //   throw new IOException("Negative key-length not allowed: " + keyLength + 
      //                         " for " + key);
      // }

      // // Append the 'value'
      // valueSerializer.serialize(value);
      // int valueLength = buffer.getLength() - keyLength;
      // if (valueLength < 0) {
      //   throw new IOException("Negative value-length not allowed: " + 
      //                         valueLength + " for " + value);
      // }
      // 
      // // Write the record out
      // WritableUtils.writeVInt(out, keyLength);                  // key length
      // WritableUtils.writeVInt(out, valueLength);                // value length
      // out.write(buffer.getData(), 0, buffer.getLength());       // data

      // // Reset
      // buffer.reset();
      // 
      // // Update bytes written
      // decompressedBytesWritten += keyLength + valueLength + 
      //                             WritableUtils.getVIntSize(keyLength) + 
      //                             WritableUtils.getVIntSize(valueLength);
      ++numRecordsWritten;
    }
    
    public void append(DataInputBuffer key, DataInputBuffer value)
    throws IOException {
      throw new UnsupportedOperationException();
    //  int keyLength = key.getLength() - key.getPosition();
    //  if (keyLength < 0) {
    //    throw new IOException("Negative key-length not allowed: " + keyLength + 
    //                          " for " + key);
    //  }
    //  
    //  int valueLength = value.getLength() - value.getPosition();
    //  if (valueLength < 0) {
    //    throw new IOException("Negative value-length not allowed: " + 
    //                          valueLength + " for " + value);
    //  }

    //  WritableUtils.writeVInt(out, keyLength);
    //  WritableUtils.writeVInt(out, valueLength);
    //  out.write(key.getData(), key.getPosition(), keyLength); 
    //  out.write(value.getData(), value.getPosition(), valueLength); 

    //  // Update bytes written
    //  decompressedBytesWritten += keyLength + valueLength + 
    //                  WritableUtils.getVIntSize(keyLength) + 
    //                  WritableUtils.getVIntSize(valueLength);
    //  ++numRecordsWritten;
    }
    
    public long getRawLength() {
      return decompressedBytesWritten;
    }
    
    public long getCompressedLength() {
      return compressedBytesWritten;
    }

}
