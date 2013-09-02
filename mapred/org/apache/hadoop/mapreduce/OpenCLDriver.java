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
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;

import org.apache.hadoop.mapreduce.OpenCLDriver;
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

public class OpenCLDriver {
  public static long inputsRead = -1L;
  public static long processingStart = -1L;
  public static long processingFinish = -1L;
  private final TaskInputOutputContext context;
  private final Class kernelClass;
  private final HadoopOpenCLContext clContext;
  private final Configuration conf;

  public OpenCLDriver(String type, TaskInputOutputContext context, Class kernelClass) {
    this.clContext = new HadoopOpenCLContext(type, context);
    this.context = context;
    this.kernelClass = kernelClass;
    this.conf = context.getConfiguration();
  }

  public static void hadoopclLog(Configuration conf, String str) throws IOException {
      String taskId = conf.get("mapred.task.id");
      String[] parts = taskId.split("_");
      String attemptId = parts[4]+"-"+parts[5];

      FileWriter fstream = new FileWriter("/scratch/jmg3/"+attemptId, true);
      BufferedWriter out = new BufferedWriter(fstream);
      out.write(str+"\n");
      out.close();
  }

  public static void hadoopclLog(Configuration conf) throws IOException {

      String tmp = "";
      for(StackTraceElement ste : Thread.currentThread().getStackTrace()) {
          tmp = tmp + ste.toString()+"\n";
      }
      hadoopclLog(conf, tmp);
  }

  public HadoopCLAccumulatedProfile javaRun() throws IOException, InterruptedException {
    HadoopCLKernel kernel = null;
    try {
        kernel = (HadoopCLKernel)kernelClass.newInstance();
        kernel.init(clContext);
        kernel.setGlobals(this.clContext.getGlobalsInd(), this.clContext.getGlobalsVal(),
                this.clContext.getGlobalIndices(), this.clContext.getNGlobals());
    } catch(Exception ex) {
        throw new RuntimeException(ex);
    }

    return kernel.javaProcess(this.context);
  }

  private String profilesToString(HadoopCLAccumulatedProfile profile) {
      StringBuffer sb = new StringBuffer();
      sb.append("DIAGNOSTICS: ");
      sb.append(this.clContext.typeName());
      sb.append("(");
      sb.append(this.clContext.getDeviceString());
      sb.append("), ");
      sb.append(profile.toString());
      return sb.toString();
  }

  private String profilesToString(long overallTime,
          List<HadoopCLBuffer.Profile> profiles) {
      StringBuffer sb = new StringBuffer();
      sb.append("DIAGNOSTICS: ");
      sb.append(this.clContext.typeName());
      sb.append("(");
      sb.append(this.clContext.getDeviceString());
      sb.append("), overallTime=");
      sb.append(overallTime);
      sb.append(" ms");
      sb.append(HadoopCLBuffer.Profile.listToString(profiles));
      return sb.toString();
  }

  /**
   * Expert users can override this method for more complete control over the
   * execution of the Mapper.
   * @param context
   * @throws IOException
   */
  public void run() throws IOException, InterruptedException {

    OpenCLDriver.processingFinish = -1;
    OpenCLDriver.processingStart = System.currentTimeMillis();
    OpenCLDriver.inputsRead = 0;

    if(this.clContext.getDevice() == null) {
        long start = System.currentTimeMillis();
        HadoopCLAccumulatedProfile javaProfile = javaRun();
        long stop = System.currentTimeMillis();
        System.err.println(profilesToString(javaProfile));
        OpenCLDriver.processingFinish = System.currentTimeMillis();
        return;
    }

    long start = System.currentTimeMillis();

    HadoopCLKernel kernel = null;
    HadoopCLBuffer buffer = null;

    try {
        kernel = (HadoopCLKernel)kernelClass.newInstance();
        kernel.init(clContext);
        kernel.setGlobals(this.clContext.getGlobalsInd(), this.clContext.getGlobalsVal(),
                this.clContext.getGlobalIndices(), this.clContext.getNGlobals());

        buffer = (HadoopCLBuffer)kernel.getBufferClass().newInstance();
        buffer.init(kernel.getOutputPairsPerInput(), clContext);
    } catch(Exception ex) {
        throw new RuntimeException(ex);
    }

    ToOpenCLThread.toRunFromMain = new LinkedList<HadoopCLBuffer>();
    ToOpenCLThread.toRunFromHadoop = new LinkedList<HadoopCLBuffer>();
    ToHadoopThread.toWrite = new LinkedList<HadoopCLBuffer>();
    ToHadoopThread.written = new LinkedList<HadoopCLBuffer>();

    ToHadoopThread th0 = new ToHadoopThread(this.clContext, kernel);
    Thread hadoopThread = new Thread(th0);

    ToOpenCLThread runner = new ToOpenCLThread(kernel, clContext);
    Thread openclThread = new Thread(runner);

    openclThread.start();
    hadoopThread.start();

    List<HadoopCLBuffer.Profile> profiles = new ArrayList<HadoopCLBuffer.Profile>();
    buffer.getProfile().startRead();

    while (this.context.nextKeyValue()) {
        if(buffer.isFull(this.context)) {
            profiles.add(buffer.getProfile());
            HadoopCLBuffer newBuffer;
            try {
                synchronized(ToHadoopThread.written) {
                    newBuffer = ToHadoopThread.written.poll();
                }
                if(newBuffer == null) {
                    newBuffer = (HadoopCLBuffer)kernel.getBufferClass().newInstance();
                    newBuffer.init(kernel.getOutputPairsPerInput(), clContext);
                } else {
                    newBuffer.reset();
                }
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            }
            buffer.transferBufferedValues(newBuffer);
            buffer.getProfile().stopRead();
            ToOpenCLThread.addWorkFromMain(buffer);
            buffer = newBuffer;
            buffer.getProfile().startRead();
        }

        buffer.addKeyAndValue(this.context);
        OpenCLDriver.inputsRead++;
    }

    if(buffer.hasWork()) {
        buffer.getProfile().stopRead();
        profiles.add(buffer.getProfile());
        ToOpenCLThread.addWorkFromMain(buffer);
    }

    ToOpenCLThread.addWorkFromMain(null);

    openclThread.join();
    hadoopThread.join();

    OpenCLDriver.processingFinish = System.currentTimeMillis();

    long stop = System.currentTimeMillis();
    System.out.println(profilesToString(stop-start, profiles));
    // System.err.println(profiler.toString(OpenCLDriver.inputsRead));
  }

}
