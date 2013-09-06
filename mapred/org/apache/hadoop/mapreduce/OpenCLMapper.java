/** * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
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

package org.apache.hadoop.mapreduce;

import java.util.HashSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;
import java.io.*;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader.BinaryKeyValues;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.*;

import java.lang.Float;
import java.lang.Class;

import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.internal.opencl.OpenCLPlatform;
import com.amd.aparapi.device.OpenCLDevice;

public class OpenCLMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private static final HashSet<String> supportedKeys;
  private static final HashSet<String> supportedValues;
  static {
      supportedKeys = new HashSet<String>();
      supportedValues = new HashSet<String>();
      supportedKeys.add("org.apache.hadoop.io.IntWritable");
      supportedKeys.add("org.apache.hadoop.io.FloatWritable");
      supportedKeys.add("org.apache.hadoop.io.DoubleWritable");
      supportedKeys.add("org.apache.hadoop.io.LongWritable");
      supportedKeys.add("org.apache.hadoop.io.PairWritable");
      supportedKeys.add("org.apache.hadoop.io.UniquePairWritable");
      supportedValues.add("org.apache.hadoop.io.IntWritable");
      supportedValues.add("org.apache.hadoop.io.FloatWritable");
      supportedValues.add("org.apache.hadoop.io.DoubleWritable");
      supportedValues.add("org.apache.hadoop.io.LongWritable");
      supportedValues.add("org.apache.hadoop.io.PairWritable");
      supportedValues.add("org.apache.hadoop.io.UniquePairWritable");
      supportedValues.add("org.apache.hadoop.io.SparseVectorWritable");
  }

  /**
   * Expert users can override this method for more complete control over the
   * execution of the Mapper.
   * @param context
   * @throws IOException
   */
  public void run(Context context) throws IOException, InterruptedException {
      System.err.println("CHECKPOINT AA");
    setup(context);
      System.err.println("CHECKPOINT BB");

    String keyClass = context.getKeyClass();
    String valueClass = context.getValueClass();

    if(!supportedKeys.contains(keyClass)) {
        throw new RuntimeException("Unsupported key type "+keyClass);
    }

    if(!supportedValues.contains(valueClass)) {
        throw new RuntimeException("Unsupported value type \'"+valueClass+"\'");
    }
      System.err.println("CHECKPOINT DD");

    OpenCLDriver driver = null;
    try {
        driver = new OpenCLDriver("mapper", context,  context.getOCLMapperClass());
    } catch(java.lang.ClassNotFoundException ce) {
        System.err.println("ERROR CHECKPOINT");
        ce.printStackTrace();
        throw new RuntimeException("Failed to load mapper kernel class");
    }

    System.err.println("CHECKPOINT EE");

    driver.run();

    System.err.println("CHECKPOINT FF");

    cleanup(context);
  }

}
