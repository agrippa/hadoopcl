package org.apache.hadoop.mapreduce;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
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
  private final int nKernels;
  private final int nInputBuffers;
  private final int nOutputBuffers;
  public static final boolean profileMemory = false;
  public static HadoopCLLogger logger = null;
  // public static final HadoopCLLogger logger = new HadoopCLLogger(false);

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

    this.nKernels = this.clContext.getNKernels();
    this.nInputBuffers = this.clContext.getNInputBuffers();
    this.nOutputBuffers = this.clContext.getNOutputBuffers();
    logger = new HadoopCLLogger(this.clContext.enableProfilingPrints());
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

  public IHadoopCLAccumulatedProfile javaRun() throws IOException, InterruptedException {
    HadoopCLKernel kernel = null;
    try {
        kernel = (HadoopCLKernel)kernelClass.newInstance();
        kernel.init(clContext);
        kernel.setGlobals(this.clContext.getGlobalsInd(),
                this.clContext.getGlobalsVal(),
                this.clContext.getGlobalIndices(), this.clContext.getNGlobals(),
                this.clContext.getGlobalsMapInd(), this.clContext.getGlobalsMapVal(),
                this.clContext.getGlobalsMap(),
                this.clContext.nGlobalBuckets());
    } catch(Exception ex) {
        throw new RuntimeException(ex);
    }

    return kernel.javaProcess(this.context);
  }

  private String profilesToString(IHadoopCLAccumulatedProfile profile,
          long startupTime) {
      if (this.clContext.doHighLevelProfiling()) {
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
      } else {
          return "";
      }
  }

  private String profilesToString(long overallTime, long startupTime,
          List<HadoopCLProfile> profiles) {
      StringBuffer sb = new StringBuffer();
      sb.append("DIAGNOSTICS: ");
      sb.append(this.clContext.typeName());
      sb.append("(");
      sb.append(this.clContext.getDeviceString());
      sb.append("), runTime=");
      sb.append(overallTime);
      sb.append(" ms, startupTime=");
      sb.append(startupTime);
      sb.append(" ms");
      sb.append(profiles.get(0).listToString(profiles));
      return sb.toString();
  }

  // private String getDetailedSpaceStats(final List<HadoopCLBuffer> globalSpace) {
  //     StringBuffer sb = new StringBuffer();
  //     long sum = 0;
  //     synchronized(globalSpace) {
  //         for(HadoopCLBuffer b : globalSpace) {
  //             if (!b.initialized()) continue;
  //             long space = b.space();
  //             sum += space;
  //             if (b instanceof HadoopCLInputBuffer) {
  //                 sb.append("[ input "+space+" "+(b.inUse() ? "used" : "free")+" ] ");
  //             } else if (b instanceof HadoopCLOutputBuffer) {
  //                 sb.append("[ output "+space+" "+(b.inUse() ? "used" : "free")+" ] ");
  //             } else {
  //                 throw new RuntimeException("Invalid buffer type detected: "+b.getClass().toString());
  //             }
  //         }
  //     }
  //     sb.append("Total = "+sum+" bytes");
  //     return sb.toString();
  // }

  private int nAllocatedInputBuffers = 0;
  private HadoopCLInputBuffer allocateNewInputBuffer(Class<? extends HadoopCLInputBuffer> toInstantiate) {
      HadoopCLInputBuffer buffer;
      try {
          buffer = toInstantiate.newInstance();
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
      nAllocatedInputBuffers++;
      return buffer;
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
    if (this.clContext.isMapper()) {
        OpenCLDriver.inputsRead = 0;
    }

    long startupTime = System.currentTimeMillis() - this.startTime;
    // LOG:PROFILE
    // logger.log("entering run", this.clContext);

    if(this.clContext.getDevice() == null) {
        IHadoopCLAccumulatedProfile javaProfile = javaRun();
        String profileStr = profilesToString(javaProfile, startupTime);
        if (profileStr.length() > 0) {
          System.out.println(profileStr);
        }
        OpenCLDriver.processingFinish = System.currentTimeMillis();
        // LOG:PROFILE
        // logger.log("exiting run", this.clContext);
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

    final ConcurrentLinkedQueue<HadoopCLInputBuffer> inputManager = new ConcurrentLinkedQueue<HadoopCLInputBuffer>();
    int nAllocatedInputBuffers = 0;
    // final BufferManager<HadoopCLInputBuffer> inputManager;
    final BufferManager<HadoopCLOutputBuffer> outputManager;
    final KernelManager kernelManager;
    int bufferCounter = 0;

    BufferRunner bufferRunner = null;
    Thread bufferRunnerThread = null;

    try {
        kernel = (HadoopCLKernel)kernelClass.newInstance();
        kernel.init(clContext);
        kernel.setGlobals(this.clContext.getGlobalsInd(),
                this.clContext.getGlobalsVal(),
                this.clContext.getGlobalIndices(), this.clContext.getNGlobals(),
                this.clContext.getGlobalsMapInd(), this.clContext.getGlobalsMapVal(),
                this.clContext.getGlobalsMap(),
                this.clContext.nGlobalBuckets());

        // inputManager = new BufferManager<HadoopCLInputBuffer>("\""+this.clContext.typeName()+" inputs\"", nInputBuffers,
        //     kernel.getInputBufferClass(), globalSpace, false, this.clContext);
        outputManager = new BufferManager<HadoopCLOutputBuffer>("\""+this.clContext.typeName()+" outputs\"", nOutputBuffers,
            kernel.getOutputBufferClass(), globalSpace, this.clContext);
        kernelManager = new KernelManager("\""+this.clContext.typeName()+"kernels\"", nKernels,
                kernelClass, this.clContext);

        bufferRunner = new BufferRunner(kernelClass, inputManager, outputManager,
                kernelManager, clContext);
        bufferRunnerThread = new Thread(bufferRunner);
        bufferRunnerThread.start();

        buffer = allocateNewInputBuffer(kernel.getInputBufferClass());
        // BufferManager.TypeAlloc<HadoopCLInputBuffer> newBufferContainer = inputManager.alloc();
        // buffer = newBufferContainer.obj();
        buffer.init(kernel.getOutputPairsPerInput(), clContext);
        buffer.tracker = new HadoopCLGlobalId(bufferCounter++);
        buffer.resetProfile();
    } catch(Exception ex) {
        throw new RuntimeException(ex);
    }

    
    buffer.getProfile().startRead(buffer);

    if (this.context.supportsBulkReads()) {
       //  HadoopCLDataInput stream = this.context.getBulkReader();
       //  while (stream.hasMore()) {
       //      int nread = buffer.bulkFill(stream);

       //      buffer.getProfile().addItemsProcessed(nread);
       //      if (this.clContext.isMapper()) {
       //          OpenCLDriver.inputsRead+=nread;
       //      }

       //      if (buffer.isFull(this.context)) {
       //          buffer.getProfile().stopRead(buffer);
       //          runner.addWork(buffer);

       //          BufferManager.TypeAlloc<HadoopCLInputBuffer> newBufferContainer = inputManager.alloc();
       //          buffer = newBufferContainer.obj();
       //          if (newBufferContainer.isFresh()) {
       //              buffer.init(kernel.getOutputPairsPerInput(), clContext);
       //          } else {
       //              buffer.reset();
       //          }
       //          buffer.tracker = new HadoopCLGlobalId(bufferCounter++);
       //          buffer.resetProfile();
       //          buffer.getProfile().startRead(buffer);
       //      }
       //  }
    } else {
        int itemCount = 0;
        while (this.context.nextKeyValue()) {
            if (buffer.isFull(this.context)) {

                System.gc();
                // if (OpenCLDriver.profileMemory) {
                //     System.err.println(getDetailedSpaceStats(globalSpace));
                // }
                // long spaceEstimate = estimateSpace(inputManager,
                //     outputManager, th0, runner, buffer);
                // System.err.println("DIAGNOSTICS: OpenCLDriver estimating space usage of "+spaceEstimate+" bytes");

                buffer.getProfile().stopRead(buffer);
                buffer.getProfile().addItemsProcessed(itemCount);
                bufferRunner.addWork(buffer);
                itemCount = 0;

                // LOG:PROFILE
                // logger.log("start allocating input", this.clContext);
                if (nAllocatedInputBuffers < this.nInputBuffers) {
                    buffer = allocateNewInputBuffer(kernel.getInputBufferClass());
                    buffer.init(kernel.getOutputPairsPerInput(), clContext);
                } else {
                    buffer = inputManager.poll();
                    if (buffer == null) {
                        synchronized(inputManager) {
                            while (inputManager.isEmpty()) {
                                inputManager.wait();
                            }
                        }
                        buffer = inputManager.poll();
                    }
                    buffer.reset();
                }

                // BufferManager.TypeAlloc<HadoopCLInputBuffer> newBufferContainer = inputManager.alloc();
                // LOG:PROFILE
                // logger.log("done allocating input", this.clContext);
                // buffer = newBufferContainer.obj();
                // if (newBufferContainer.isFresh()) {
                //     buffer.init(kernel.getOutputPairsPerInput(), clContext);
                // } else {
                //     buffer.reset();
                // }
                buffer.tracker = new HadoopCLGlobalId(bufferCounter++);
                buffer.resetProfile();
                buffer.getProfile().startRead(buffer);
            }

            buffer.addKeyAndValue(this.context);
            if (this.clContext.isMapper()) {
                OpenCLDriver.inputsRead++;
            }
            itemCount++;
        }
        buffer.getProfile().stopRead(buffer);
        buffer.getProfile().addItemsProcessed(itemCount);
    }

    this.context.signalDoneReading();

    if(buffer.hasWork()) {
        bufferRunner.addWork(buffer);
    }

    bufferRunner.addWork(MainDoneMarker.SINGLETON);

    bufferRunnerThread.join();
    kernelManager.dispose();

    OpenCLDriver.processingFinish = System.currentTimeMillis();

    long stop = System.currentTimeMillis();
    // LOG:PROFILE
    // logger.log("exiting run", this.clContext);
    String profileStr = profilesToString(stop-start, startupTime, bufferRunner.profiles());
    if (profileStr.length() > 0) {
      System.out.println(profileStr);
    }
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
