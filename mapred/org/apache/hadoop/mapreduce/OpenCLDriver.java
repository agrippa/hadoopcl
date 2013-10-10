package org.apache.hadoop.mapreduce;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;
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
  public static final int nInputBuffers = 4;
  public static final int nOutputBuffers = 5;
  public static final boolean profileMemory = true;

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

  private String getDetailedSpaceStats(final List<HadoopCLBuffer> globalSpace) {
      StringBuffer sb = new StringBuffer();
      long sum = 0;
      synchronized(globalSpace) {
          for(HadoopCLBuffer b : globalSpace) {
              if (!b.initialized()) continue;
              long space = b.space();
              sum += space;
              if (b instanceof HadoopCLInputBuffer) {
                  sb.append("[ input "+space+" "+(b.inUse() ? "used" : "free")+" ] ");
              } else if (b instanceof HadoopCLOutputBuffer) {
                  sb.append("[ output "+space+" "+(b.inUse() ? "used" : "free")+" ] ");
              } else {
                  throw new RuntimeException("Invalid buffer type detected: "+b.getClass().toString());
              }
          }
      }
      sb.append("Total = "+sum+" bytes");
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
        System.out.println(profilesToString(javaProfile));
        OpenCLDriver.processingFinish = System.currentTimeMillis();
        return;
    }

    long start = System.currentTimeMillis();

    HadoopCLKernel kernel = null;
    HadoopCLInputBuffer buffer = null;
    final List<HadoopCLBuffer> globalSpace;
    if (OpenCLDriver.profileMemory) {
        globalSpace = new LinkedList<HadoopCLBuffer>();
    } else {
        globalSpace = null;
    }
    BufferManager<HadoopCLInputBuffer> inputManager;
    BufferManager<HadoopCLOutputBuffer> outputManager;

    try {
        kernel = (HadoopCLKernel)kernelClass.newInstance();
        kernel.init(clContext);
        kernel.setGlobals(this.clContext.getGlobalsInd(), this.clContext.getGlobalsVal(),
                this.clContext.getGlobalIndices(), this.clContext.getNGlobals());

        inputManager = new BufferManager<HadoopCLInputBuffer>("inputs", nInputBuffers,
            kernel.getInputBufferClass(), globalSpace);
        outputManager = new BufferManager<HadoopCLOutputBuffer>("outputs", nOutputBuffers,
            kernel.getOutputBufferClass(), globalSpace);

        BufferManager.BufferTypeAlloc<HadoopCLInputBuffer> newBufferContainer = inputManager.alloc();
        buffer = newBufferContainer.obj();
        buffer.init(kernel.getOutputPairsPerInput(), clContext);
    } catch(Exception ex) {
        throw new RuntimeException(ex);
    }
    int countBuffers = 1;

    ToOpenCLThread.toRun = new HadoopCLLimitedQueue<HadoopCLInputBuffer>();
    ToHadoopThread.toWrite = new HadoopCLLimitedQueue<HadoopCLOutputBuffer>();

    ToHadoopThread th0 = new ToHadoopThread(this.clContext, outputManager, kernel);
    Thread hadoopThread = new Thread(th0);

    ToOpenCLThread runner = new ToOpenCLThread(kernel, inputManager, outputManager, clContext);
    Thread openclThread = new Thread(runner);

    openclThread.start();
    hadoopThread.start();

    List<HadoopCLBuffer.Profile> profiles = new ArrayList<HadoopCLBuffer.Profile>();
    buffer.getProfile().startRead();

    while (this.context.nextKeyValue()) {
        if (buffer.isFull(this.context)) {
            profiles.add(buffer.getProfile());

            System.gc();
            if (OpenCLDriver.profileMemory) {
                System.err.println(getDetailedSpaceStats(globalSpace));
            }
            // printDetailedSpaceUsage(inputManager, outputManager, th0, runner, buffer);
            // long spaceEstimate = estimateSpace(inputManager,
            //     outputManager, th0, runner, buffer);
            // System.err.println("DIAGNOSTICS: OpenCLDriver estimating space usage of "+spaceEstimate+" bytes");
            BufferManager.BufferTypeAlloc<HadoopCLInputBuffer> newBufferContainer = inputManager.alloc();
            HadoopCLInputBuffer newBuffer = newBufferContainer.obj();

            if (newBufferContainer.isFresh()) {
                newBuffer.init(kernel.getOutputPairsPerInput(), clContext);
            } else {
                newBuffer.reset();
            }

            buffer.transferBufferedValues(newBuffer);
            buffer.getProfile().stopRead();
            ToOpenCLThread.addWorkFromMain(buffer);
            buffer = newBuffer;
            buffer.getProfile().startRead();
        }

        buffer.addKeyAndValue(this.context);
        OpenCLDriver.inputsRead++;
        buffer.getProfile().addItemProcessed();
    }
    buffer.getProfile().stopRead();

    if(buffer.hasWork()) {
        profiles.add(buffer.getProfile());
        ToOpenCLThread.addWorkFromMain(buffer);
    }

    ToOpenCLThread.addWorkFromMain(null);

    openclThread.join();
    hadoopThread.join();

    OpenCLDriver.processingFinish = System.currentTimeMillis();

    long stop = System.currentTimeMillis();
    System.out.println(profilesToString(stop-start, profiles));
  }
}
