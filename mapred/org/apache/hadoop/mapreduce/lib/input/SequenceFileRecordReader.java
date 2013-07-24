/** * Licensed to the Apache Software Foundation (ASF) under one * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.SequenceFileAsBinaryOutputFormat;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FSDataInputStream;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;

/** An {@link RecordReader} for {@link SequenceFile}s. */
public class SequenceFileRecordReader<K, V> extends RecordReader<K, V> {
  private SequenceFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  private K key = null;
  private V value = null;
  protected Configuration conf;
  
  @Override
  public void initialize(InputSplit split, 
                         TaskAttemptContext context
                         ) throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    conf = context.getConfiguration();    
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    this.in = new SequenceFile.Reader(fs, path, conf);
    this.end = fileSplit.getStart() + fileSplit.getLength();

    if (fileSplit.getStart() > in.getPosition()) {
      in.sync(fileSplit.getStart());                  // sync to start
    }

    this.start = in.getPosition();
    more = start < end;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!more) {
      return false;
    }
    long pos = in.getPosition();
    key = (K) in.next(key);
    if (key == null || (pos >= end && in.syncSeen())) {
      more = false;
      key = null;
      value = null;
    } else {
      value = (V) in.getCurrentValue(value);
    }
    return more;
  }

  @Override
  public K getCurrentKey() {
    return key;
  }
  
  @Override
  public V getCurrentValue() {
    return value;
  }

  public String getKeyClassName() {
      return in.getKeyClassName();
  }
  public String getValueClassName() {
      return in.getValueClassName();
  }

  public int getAvailable() throws IOException {
    FSDataInputStream rawFile = in.getIn();
    return rawFile.available();
  }

  public BinaryKeyValues getChunkedKeyValues() throws IOException {
    FSDataInputStream rawFile = in.getIn();
    byte[] raw = new byte[rawFile.available()];

    int read;
    int total_read = 0;
    while((read = rawFile.read(raw, total_read, raw.length-total_read)) != -1) {
        total_read += read;
    }
    in.close();
    return new BinaryKeyValues(raw, total_read);
  }

  public long getTotal() { 
      return end-start;
  }
  
  /**
   * Return the progress within the input split
   * @return 0.0 to 1.0 of the input byte range
   */
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException { in.close(); }

  public static class BinaryKeyValues {
      private final byte[] raw;
      private final int rawLength;
      private int recordSize;
      private byte[] sync;
      private int syncIntervalLength; // without sync hash
      private int firstSyncOffset;

      public BinaryKeyValues(byte[] setRaw, int setRawLength) {
          this.raw = setRaw;
          this.rawLength = setRawLength;
          this.recordSize = -1;
          this.sync = null;
          this.syncIntervalLength = -1;
          this.firstSyncOffset = -1;
      }

      public int getRawLength() {
          return this.rawLength;
      }

      public byte[] getRaw() {
          return raw;
      }

      public int getFirstSyncOffset() {
          if(this.firstSyncOffset == -1) {
              this.getSyncIntervalLength();
          }
          return this.firstSyncOffset;
      }

      public int getSyncIntervalLength() {
          if(this.syncIntervalLength != -1) {
              return this.syncIntervalLength;
          }

          ByteBuffer buf;
          int pos = 0;

          while(true) {
              int length;
              try {
                  buf = ByteBuffer.wrap(raw, pos, 4);
                  length = buf.getInt();
              } catch(Exception e) {
                  return -1;
              }

              if(length == -1) {
                  break;
              } else {
                  pos += (4 + 4 + length);
              }
          }

          this.firstSyncOffset = pos;
          pos = pos + 4 + 16;
          int start = pos;

          while(true) {
              int length;
              try {
                  buf = ByteBuffer.wrap(raw, pos, 4);
                  length = buf.getInt();
              } catch(Exception e) {
                  break;
              }

              if(length == -1) {
                  this.syncIntervalLength = pos - start;
                  return this.syncIntervalLength;
              } else {
                  pos = pos + 4 + 4 + length;
              }
          }
          return -1;
      }

      public int getRecordSize() {
        ByteBuffer buf;

        if(this.recordSize != -1) {
            return this.recordSize;
        }

        int pos = 0;

        int length;
        try {
            buf = ByteBuffer.wrap(raw, pos, 4); // wrap an integer
            length = buf.getInt();
        } catch(Exception e) {
            return -1;
        }
        pos += 4; // record length increment

        if(length == -1) { // sync
            try {
                pos += 16; // sync marker
                buf = ByteBuffer.wrap(raw, pos, 4);
                length = buf.getInt();
                pos += 4; // retry record length
            } catch(Exception e) {
                return -1;
            }
        }
        this.recordSize = length + 8;
        return this.recordSize; // includes recordLength, keyLength, and <key,value> lengths
      }

      public byte[] getSync() {
        ByteBuffer buf;

        if(sync != null) {
            return sync;
        }

        int pos = 0;
        while(true) {
            int length;
            try {
                buf = ByteBuffer.wrap(raw, pos, 4); // wrap an integer
                length = buf.getInt();
            } catch(Exception e) {
                return null;
            }
            pos += 4; // record length increment

            if(length == -1) { // sync
                byte[] result = new byte[16];
                for(int i = pos; i < pos + result.length; i++) {
                    result[i-pos] = raw[i];
                }
                this.sync = result;
                return this.sync;
            }

            pos += length + 4; // +4 for key length
        }
      }
  }
}

