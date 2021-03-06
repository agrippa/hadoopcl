package org.apache.hadoop.mapreduce;

import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
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
  public static int taskId = -1;
  public static int attemptId = -1;
  // public static final HadoopCLLogger logger = new HadoopCLLogger(false);

  public static final GlobalsWrapper globals = new GlobalsWrapper();
  private final GlobalsWrapper writableGlobals = new GlobalsWrapper();

  // public static long inputsRead = -1L;
  // public static long processingStart = -1L;
  // public static long processingFinish = -1L;
  private final TaskInputOutputContext context;
  private final HadoopOpenCLContext clContext;
  private final Configuration conf;
  private final long startTime;

  public static AtomicReference<MutableLongPair> globalStatus =
      new AtomicReference<MutableLongPair>(new MutableLongPair());

  private static MutableLongPair getNext(MutableLongPair curr, long n, long p) {
      if (curr.nInputs == -1L) {
          return new MutableLongPair(n, p);
      } else {
          return new MutableLongPair(curr.nInputs + n, curr.processingTime + p);
      }
  }
  public static void addProgress(long n, long p) {
      MutableLongPair curr, next;

      do {
          curr = globalStatus.get();
          next = getNext(curr, n, p);
      } while (!globalStatus.compareAndSet(curr, next));
  }

  public OpenCLDriver(String type, TaskInputOutputContext context) {
    this.startTime = System.currentTimeMillis();
    this.clContext = new HadoopOpenCLContext(type, context, globals, writableGlobals);
    this.context = context;
    this.conf = context.getConfiguration();

    this.nKernels = this.clContext.getNKernels();
    this.nInputBuffers = this.clContext.getNInputBuffers();
    this.nOutputBuffers = this.clContext.getNOutputBuffers();
    logger = new HadoopCLLogger(this.clContext.enableProfilingPrints());
    if (taskId == -1) {
        taskId = context.getTaskAttemptID().getTaskID().getId();
        attemptId = context.getTaskAttemptID().getId();
    }
  }

  public static String printStackTrace() {
      StringBuilder sb = new StringBuilder();
      StackTraceElement[] trace = Thread.currentThread().getStackTrace();
      for (int i = 1; i < trace.length; i++) {
          sb.append(trace[i].toString()+"\n");
      }
      return sb.toString();
  }

  public IHadoopCLAccumulatedProfile javaRun(final boolean shouldIncr) throws IOException, InterruptedException {
    HadoopCLKernel kernel = null;
    try {
        kernel = this.clContext.thisKernelConstructor.newInstance(this.clContext,
            -1);
    } catch(Exception ex) {
        throw new RuntimeException(ex);
    }
    final IHadoopCLAccumulatedProfile profile = kernel.javaProcess(this.context,
        shouldIncr);

    if (kernel.nWritables > 0) {
        try {
            final TaskInputOutputContext ctx = this.clContext.getContext();
            final Class valueClass;
            if (this.clContext.isMapper() || this.clContext.isCombiner()) {
                valueClass = ctx.getMapOutputValueClass();
            } else {
                valueClass = ctx.getOutputValueClass();
            }
            final Constructor<WritableComparable> constructor =
                valueClass.getConstructor(new Class[] { int[].class,
                    Integer.class, float[].class, Integer.class, Integer.class });

            for (int i = 0; i < kernel.nWritables; i++) {
                final int writableStart = kernel.writableIndices[i];
                final int writableEnd;
                if (i == kernel.nWritables - 1) {
                    writableEnd = kernel.writableInd.length;
                } else {
                    writableEnd = kernel.writableIndices[i + 1];
                }
                final int writableLength = writableEnd - writableStart;
                final WritableComparable val = constructor.newInstance(
                        kernel.writableInd, kernel.writableIndices[i],
                        kernel.writableVal, kernel.writableIndices[i], 
                        writableLength);
                ctx.write(new IntWritable(i), val);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    return profile;
  }

  private String profilesToString(IHadoopCLAccumulatedProfile profile,
          long startupTime, long totalTime) {
      StringBuilder sb = new StringBuilder();
      sb.append("DIAGNOSTICS: ");
      sb.append(this.clContext.verboseTypeName());
      sb.append("(");
      sb.append(this.clContext.getDeviceString());
      sb.append("), runTime = ");
      sb.append(totalTime);
      sb.append(" ms, startupTime = ");
      sb.append(startupTime);
      sb.append(" ms");
      if (this.clContext.doHighLevelProfiling()) {
          sb.append(", "+profile.toString());
      }
      return sb.toString();
  }

  private String profilesToString(long overallTime, long startupTime,
          List<HadoopCLProfile> profiles) {
      StringBuilder sb = new StringBuilder();
      sb.append("DIAGNOSTICS: ");
      sb.append(this.clContext.verboseTypeName());
      sb.append("(");
      sb.append(this.clContext.getDeviceString());
      sb.append("), runTime = ");
      sb.append(overallTime);
      sb.append(" ms, startupTime = ");
      sb.append(startupTime);
      sb.append(" ms");
      if (!profiles.isEmpty()) {
          sb.append(profiles.get(0).listToString(profiles));
      }
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
  private HadoopCLInputBuffer allocateNewInputBuffer() {
      HadoopCLInputBuffer buffer;
      try {
          buffer = this.clContext.inputBufferConstructor.newInstance(
              this.clContext, nAllocatedInputBuffers);
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
      nAllocatedInputBuffers++;
      return buffer;
  }

  private int bufferCounter = 0;
  private HadoopCLInputBuffer handleFullBuffer(HadoopCLInputBuffer buffer,
          int itemCount, BufferRunner bufferRunner,
          final List<HadoopCLInputBuffer> inputManager,
          TaskInputOutputContext ctx)
              throws InterruptedException, IOException {
      HadoopCLInputBuffer newBuffer;
      System.gc();
      buffer.getProfile().stopRead(buffer);
      buffer.getProfile().addItemsProcessed(itemCount);

      if (this.clContext.isReducer()) {
          HadoopCLInputReducerBuffer reducerBuffer = (HadoopCLInputReducerBuffer)buffer;
          /*
           * If the next key is the same as the last and we're in a reducer, we
           * must wait to process the buffered values for the current key so
           * that they can all be run together.
           */
          if (reducerBuffer.sameAsLastKey(ctx.getCurrentKey())) {
              reducerBuffer.removeLastKey();
          }
      }

      // if (this.clContext.isCombiner() && ((ReduceContext)this.clContext.getContext()).shouldPrint()) {
      //     buffer.printContents();
      // }

      bufferRunner.addWork(buffer);

       // LOG:PROFILE
       // logger.log("start allocating input", this.clContext);
      if (this.nAllocatedInputBuffers < this.nInputBuffers) {
          newBuffer = allocateNewInputBuffer();
      } else {
          synchronized (inputManager) {
              while (inputManager.isEmpty()) {
                  inputManager.wait();
              }
              newBuffer = inputManager.remove(0);
          }
      }
      newBuffer.reset();
      // LOG:PROFILE
      // logger.log("done allocating input", this.clContext);

      newBuffer.tracker = new HadoopCLGlobalId(bufferCounter++);
      newBuffer.resetProfile();
      newBuffer.getProfile().startRead(newBuffer);

      if (this.clContext.isReducer() && ((HadoopCLInputReducerBuffer)buffer).hasKeyLeftover()) {
          ((HadoopCLInputReducerBuffer)newBuffer).transferLastKey((HadoopCLInputReducerBuffer)buffer);
      }

      return newBuffer;
  }

  /**
   * Expert users can override this method for more complete control over the
   * execution of the Mapper.
   * @param context
   * @throws IOException
   */
  public void run() throws IOException, InterruptedException {

    final boolean isCombiner = this.clContext.isCombiner();

    final long startupTime = System.currentTimeMillis() - this.startTime;
    // LOG:PROFILE
    // logger.log("entering run", this.clContext);

    if(this.clContext.getDevice() == null) {
        final long start = System.currentTimeMillis();
        IHadoopCLAccumulatedProfile javaProfile = javaRun(!isCombiner);
        final long stop = System.currentTimeMillis();
        // OpenCLDriver.processingFinish = System.currentTimeMillis();
        String profileStr = profilesToString(javaProfile, startupTime, stop - start);
        System.out.println(profileStr);
        // LOG:PROFILE
        // logger.log("exiting run", this.clContext);
        return;
    }

    final long start = System.currentTimeMillis();

    HadoopCLInputBuffer buffer = null;
    final List<HadoopCLBuffer> globalSpace;
    if (OpenCLDriver.profileMemory) {
        globalSpace = new LinkedList<HadoopCLBuffer>();
    } else {
        globalSpace = null;
    }

    final List<HadoopCLInputBuffer> inputManager = new LinkedList<HadoopCLInputBuffer>();
    int nAllocatedInputBuffers = 0;

    BufferRunner bufferRunner = null;
    Thread bufferRunnerThread = null;

    try {

        bufferRunner = new BufferRunner(inputManager, clContext);
        bufferRunnerThread = new Thread(bufferRunner);
        bufferRunnerThread.setName("Buffer-Runner");
        bufferRunnerThread.start();

        buffer = allocateNewInputBuffer();
        buffer.tracker = new HadoopCLGlobalId(bufferCounter++);
        buffer.resetProfile();
        buffer.reset();
    } catch(Exception ex) {
        throw new RuntimeException(ex);
    }

    buffer.getProfile().startRead(buffer);

    int itemCount = 0;
    if (this.context.supportsBulkReads()) {
        HadoopCLDataInput stream = this.context.getBulkReader();
        while (stream.hasMore()) {
            itemCount += buffer.bulkFill(stream);

            buffer.getProfile().addItemsProcessed(itemCount);

            if (buffer.isFull(this.context)) {
                buffer = handleFullBuffer(buffer, itemCount, bufferRunner, inputManager, this.context);
                itemCount = 0;
            }
        }
    } else {
        while (this.context.nextKeyValue()) {
            if (buffer.isFull(this.context)) {
                buffer = handleFullBuffer(buffer, itemCount, bufferRunner, inputManager, this.context);
                itemCount = 0;
            }

            buffer.addKeyAndValue(this.context);
            itemCount++;
        }
        buffer.getProfile().addItemsProcessed(itemCount);
    }
    buffer.getProfile().stopRead(buffer);

    this.context.signalDoneReading();

    if(buffer.hasWork()) {
        bufferRunner.addWork(buffer);
    }

    bufferRunner.addWork(new MainDoneMarker(this.clContext));

    bufferRunnerThread.join();

    final long stop = System.currentTimeMillis();
    // LOG:PROFILE
    // logger.log("exiting run", this.clContext);
    String profileStr = profilesToString(stop - start, startupTime,
        bufferRunner.profiles());
    System.out.println(profileStr);
  }

  public static class MutableLongPair {
      public final long nInputs;
      public final long processingTime;

      public MutableLongPair() {
          this.nInputs = -1L;
          this.processingTime = 0L;
      }

      public MutableLongPair(long n, long p) {
          this.nInputs = n;
          this.processingTime = p;
      }
  }
}
