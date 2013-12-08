package org.apache.hadoop.mapreduce;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.HashSet;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;
import java.security.Permission;

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
  /*
   * Reducers require a minimum of 2 input buffers because they have to allocate
   * one before making the other eligible for execution in order to transfer
   * values.
   */
  public static final int nKernels = 1;
  public static final int nInputBuffers = 3;
  public static final int nOutputBuffers = 1;
  public static final boolean profileMemory = false;

  public static final GlobalsWrapper globals = new GlobalsWrapper();

  public static long inputsRead = -1L;
  public static long processingStart = -1L;
  public static long processingFinish = -1L;
  private final TaskInputOutputContext context;
  private final Class kernelClass;
  private final HadoopOpenCLContext clContext;
  private final Configuration conf;
  private final long startTime;

  public OpenCLDriver(String type, TaskInputOutputContext context, Class kernelClass) {
    this.startTime = System.currentTimeMillis();
    this.clContext = new HadoopOpenCLContext(type, context, globals);
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
        kernel.setGlobals(this.clContext.getGlobalsInd(),
                this.clContext.getGlobalsVal(), this.clContext.getGlobalsFval(),
                this.clContext.getGlobalIndices(), this.clContext.getNGlobals(),
                this.clContext.getGlobalsMapInd(), this.clContext.getGlobalsMapVal(),
                this.clContext.getGlobalsMapFval(), this.clContext.getGlobalsMap(),
                this.clContext.nGlobalBuckets());
    } catch(Exception ex) {
        throw new RuntimeException(ex);
    }

    return kernel.javaProcess(this.context);
  }

  private String profilesToString(HadoopCLAccumulatedProfile profile,
          long startupTime) {
      StringBuffer sb = new StringBuffer();
      sb.append("DIAGNOSTICS: ");
      sb.append(this.clContext.typeName());
      sb.append("(");
      sb.append(this.clContext.getDeviceString());
      sb.append("), startupTime=");
      sb.append(startupTime);
      sb.append(" ms, ");
      sb.append(profile.toString());
      return sb.toString();
  }

  private String profilesToString(long overallTime, long startupTime,
          List<HadoopCLBuffer.Profile> profiles, long inputTimeWaiting,
          long outputTimeWaiting, long kernelTimeWaiting) {
      StringBuffer sb = new StringBuffer();
      sb.append("DIAGNOSTICS: ");
      sb.append(this.clContext.typeName());
      sb.append("(");
      sb.append(this.clContext.getDeviceString());
      sb.append("), runTime=");
      sb.append(overallTime);
      sb.append(" ms, startupTime=");
      sb.append(startupTime);
      sb.append(" ms, input buffer lag=");
      sb.append(inputTimeWaiting);
      sb.append(" ms, output buffer lag=");
      sb.append(outputTimeWaiting);
      sb.append(" ms, kernel lag=");
      sb.append(kernelTimeWaiting);
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

    long startupTime = System.currentTimeMillis() - this.startTime;

    if(this.clContext.getDevice() == null) {
        HadoopCLAccumulatedProfile javaProfile = javaRun();
        System.out.println(profilesToString(javaProfile, startupTime));
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
    KernelManager kernelManager;

    try {
        kernel = (HadoopCLKernel)kernelClass.newInstance();
        kernel.init(clContext);
        kernel.setGlobals(this.clContext.getGlobalsInd(),
                this.clContext.getGlobalsVal(), this.clContext.getGlobalsFval(),
                this.clContext.getGlobalIndices(), this.clContext.getNGlobals(),
                this.clContext.getGlobalsMapInd(), this.clContext.getGlobalsMapVal(),
                this.clContext.getGlobalsMapFval(), this.clContext.getGlobalsMap(),
                this.clContext.nGlobalBuckets());

        inputManager = new BufferManager<HadoopCLInputBuffer>("\""+this.clContext.typeName()+" inputs\"", nInputBuffers,
            kernel.getInputBufferClass(), globalSpace);
        outputManager = new BufferManager<HadoopCLOutputBuffer>("\""+this.clContext.typeName()+" outputs\"", nOutputBuffers,
            kernel.getOutputBufferClass(), globalSpace);
        kernelManager = new KernelManager("\""+this.clContext.typeName()+"kernels\"", nKernels,
                kernelClass, this.clContext);

        BufferManager.TypeAlloc<HadoopCLInputBuffer> newBufferContainer = inputManager.alloc();
        buffer = newBufferContainer.obj();
        buffer.init(kernel.getOutputPairsPerInput(), clContext);
        buffer.resetProfile();
    } catch(Exception ex) {
        throw new RuntimeException(ex);
    }

    BufferRunner runner = new BufferRunner(kernelClass, inputManager, outputManager,
            kernelManager, clContext);
    Thread thread = new Thread(runner);
    thread.start();
    
    buffer.getProfile().startRead();

    while (this.context.nextKeyValue()) {
        if (buffer.isFull(this.context)) {

            System.gc();
            if (OpenCLDriver.profileMemory) {
                System.err.println(getDetailedSpaceStats(globalSpace));
            }
            // long spaceEstimate = estimateSpace(inputManager,
            //     outputManager, th0, runner, buffer);
            // System.err.println("DIAGNOSTICS: OpenCLDriver estimating space usage of "+spaceEstimate+" bytes");

            if (this.clContext.isMapper()) {
                // No mappers have a filled in transferBufferedValues,
                // so we don't need to hold two input buffers at once
                // and can release the current one immediately before
                // requesting a new one.
                buffer.getProfile().stopRead();
                runner.addWork(buffer);

                BufferManager.TypeAlloc<HadoopCLInputBuffer> newBufferContainer = inputManager.alloc();
                buffer = newBufferContainer.obj();
                // System.err.println("Main starting buffering into "+buffer.id);
                if (newBufferContainer.isFresh()) {
                    buffer.init(kernel.getOutputPairsPerInput(), clContext);
                } else {
                    buffer.reset();
                }
            } else {
                BufferManager.TypeAlloc<HadoopCLInputBuffer> newBufferContainer = inputManager.alloc();
                HadoopCLInputBuffer newBuffer = newBufferContainer.obj();

                if (newBufferContainer.isFresh()) {
                    newBuffer.init(kernel.getOutputPairsPerInput(), clContext);
                } else {
                    newBuffer.reset();
                }

                buffer.transferBufferedValues(newBuffer);
                buffer.getProfile().stopRead();
                runner.addWork(buffer);
                buffer = newBuffer;
            }
            buffer.resetProfile();
            buffer.getProfile().startRead();
        }

        buffer.addKeyAndValue(this.context);
        OpenCLDriver.inputsRead++;
        buffer.getProfile().addItemProcessed();
    }
    buffer.getProfile().stopRead();

    if(buffer.hasWork()) {
        runner.addWork(buffer);
    }

    runner.addWork(null);

    thread.join();

    OpenCLDriver.processingFinish = System.currentTimeMillis();

    long stop = System.currentTimeMillis();
    System.out.println(profilesToString(stop-start, startupTime, runner.profiles(),
          inputManager.timeWaiting(), outputManager.timeWaiting(),
          kernelManager.timeWaiting()));
  }

  /*
  public static class NoExit extends SecurityManager {
      @Override
      public void checkPermission(Permission perm) {
      }
      @Override
      public void checkPermission(Permission perm, Object context) {
      }
      @Override
      public void checkExit(int status) {
          System.err.println("Printing stack from exit "+status);
          for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
              System.out.println("  "+ste);
          }
      }
  }
  */
}
