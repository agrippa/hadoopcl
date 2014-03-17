
package org.apache.hadoop.mapreduce;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.List;
import java.util.EnumSet;
import com.amd.aparapi.internal.opencl.OpenCLPlatform;
import com.amd.aparapi.device.OpenCLDevice;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.internal.util.OpenCLUtil;
import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SparseVectorWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.TaskInputOutputContext.ContextType;
import java.nio.IntBuffer;
import java.nio.FloatBuffer;
import java.nio.DoubleBuffer;
import java.nio.ByteBuffer;

public class HadoopOpenCLContext {

    private final boolean doHighLevelProfiling;
    private final boolean enableBufferRunnerDiagnostics;
    private final boolean enableProfilingPrints;
    private final TaskInputOutputContext hadoopContext;
    private final String type;
    private final String verboseType;
    private int threadsPerGroup;
    private OpenCLDevice device;
    private int deviceId;
    private int deviceSlot;
    private int isGPU;
    private final int inputBufferSize;
    private final int outputBufferSize;
    private final int inputValMultiplier;
    private final int inputValEleMultiplier;
    private final int preallocIntLength;
    private final int preallocFloatLength;
    private final int preallocDoubleLength;
    private final int outputBufferSpillChunk;
    private String deviceString;

    private boolean isCombiner;
    private final boolean jobHasCombiner;
    private final int nCombinerKernels;

    public final Constructor<? extends HadoopCLInputBuffer> inputBufferConstructor;
    public final Constructor<? extends HadoopCLOutputBuffer> outputBufferConstructor;
    public final Constructor<HadoopCLMapperKernel> mapperKernelConstructor;
    public final Constructor<HadoopCLReducerKernel> reducerKernelConstructor;
    public final Constructor<HadoopCLReducerKernel> combinerKernelConstructor;
    public final Constructor<? extends HadoopCLKernel> thisKernelConstructor;

    public final HadoopCLMapperKernel mapperKernel;
    public final HadoopCLReducerKernel reducerKernel;
    public final HadoopCLReducerKernel combinerKernel;
    public final HadoopCLKernel thisKernel;

    private GlobalsWrapper globals;
    
    private final int nKernels;
    private final int nInputBuffers;
    private final int nOutputBuffers;

    public HadoopOpenCLContext() {
        globals = new GlobalsWrapper();
        doHighLevelProfiling = false;
        enableBufferRunnerDiagnostics = false;
        enableProfilingPrints = false;
        hadoopContext = null;
        type = "scheduler";
        verboseType = "scheduler";
        inputBufferSize = 0;
        outputBufferSize = 0;
        inputValMultiplier = 0;
        inputValEleMultiplier = 0;
        preallocIntLength = 0;
        preallocFloatLength = 0;
        preallocDoubleLength = 0;
        outputBufferSpillChunk = 0;
        jobHasCombiner = false;
        nCombinerKernels = 0;
        inputBufferConstructor = null;
        outputBufferConstructor = null;
        mapperKernelConstructor = null;
        reducerKernelConstructor = null;
        combinerKernelConstructor = null;
        thisKernelConstructor = null;
        mapperKernel = null;
        reducerKernel = null;
        combinerKernel = null;
        thisKernel = null;
        nKernels = 0;
        nInputBuffers = nOutputBuffers = 0;
    }

    public HadoopOpenCLContext(String contextType,
        TaskInputOutputContext setHadoopContext, GlobalsWrapper globals) {

      this.hadoopContext = setHadoopContext;

      if (this.hadoopContext.getContextType() == ContextType.Combiner) {
        this.isCombiner = true;
        this.type = "combiner";
        this.verboseType = "combiner-"+this.hadoopContext.getLabel();
      } else {
        this.isCombiner = false;
        this.type = contextType;
        this.verboseType = type;
      }

      Configuration conf = this.hadoopContext.getConfiguration();
      this.nKernels = conf.getInt("opencl."+type+".nkernels", 1);
      this.nCombinerKernels = conf.getInt("opencl.combiner.nkernels", 1);
      this.nInputBuffers = conf.getInt("opencl."+type+".ninputbuffers", 3);
      this.nOutputBuffers = conf.getInt("opencl."+type+".noutputbuffers", 1);
      this.preallocIntLength = conf.getInt("opencl."+type+".prealloc.length.int", 5242880);
      this.preallocFloatLength = conf.getInt("opencl."+type+".prealloc.length.float", 5242880);
      this.preallocDoubleLength = conf.getInt("opencl."+type+".prealloc.length.double", 5242880);
      this.outputBufferSpillChunk = conf.getInt("opencl.spill.chunk", 2);
      this.enableBufferRunnerDiagnostics = conf.getBoolean("opencl.buffer.diagnostics", false);
      this.enableProfilingPrints = conf.getBoolean("opencl.profiling", false);
      this.doHighLevelProfiling = conf.getBoolean("opencl.highlevel", false);
      this.inputBufferSize = conf.getInt("opencl."+this.type+".inputBufferSize", 32768);
      this.inputValMultiplier = conf.getInt("opencl."+this.type+".val_multiplier", this.isMapper() ? 1 : 128);
      this.inputValEleMultiplier = conf.getInt("opencl."+this.type+".val_ele_multiplier", 5);
      this.outputBufferSize = conf.getInt("opencl."+this.type+".outputBufferSize", 32768);

      this.globals = globals;
      synchronized(this.globals) {
        this.globals.init(conf);
      }

      if (this.isCombiner) {
        this.deviceId = findDeviceWithType(retrieveCombinerDeviceType(conf));
      } else if(System.getProperty("opencl.device") != null) {
        this.deviceId = Integer.parseInt(System.getProperty("opencl.device"));
      } else {
        this.deviceId = 0;
      }
      this.device = findDevice(this.deviceId);

      if (System.getProperty("opencl.device_slot") != null) {
        this.deviceSlot = Integer.parseInt(System.getProperty("opencl.device_slot"));
      } else {
        this.deviceSlot = -1;
      }

      if(this.device == null) {
        this.deviceString = "java";
        this.isGPU = 0;
      } else {
        if(this.device.getType() == Device.TYPE.GPU) {
          this.deviceString = "gpu";
          this.isGPU = 1;
        } else {
          this.deviceString = "cpu";
          this.isGPU = 0;
        }
      }

      String threadsPerGroupStr = System.getProperty("opencl."+this.type+".threadsPerGroup."+this.deviceString);
      if(threadsPerGroupStr != null) {
        this.threadsPerGroup = Integer.parseInt(threadsPerGroupStr);
      } else {
        this.threadsPerGroup = 256;
      }

      try {
        final Class mapperClass = hadoopContext.getOCLMapperClass();
        final Class reducerClass = hadoopContext.getOCLReducerClass();
        final Class combinerClass = hadoopContext.getOCLCombinerClass();

        this.mapperKernelConstructor = mapperClass.getConstructor(new Class[] {
          HadoopOpenCLContext.class, Integer.class });
        this.reducerKernelConstructor = reducerClass.getConstructor(new Class[] {
          HadoopOpenCLContext.class, Integer.class });
        if (combinerClass != null) {
          this.jobHasCombiner = true;
          this.combinerKernelConstructor = combinerClass.getConstructor(new Class[] {
            HadoopOpenCLContext.class, Integer.class });
        } else {
          this.jobHasCombiner = false;
          this.combinerKernelConstructor = null;
        }

        this.mapperKernel = this.mapperKernelConstructor.newInstance(this, -1);
        this.reducerKernel = this.reducerKernelConstructor.newInstance(this, -1);
        if (this.combinerKernelConstructor != null) {
          this.combinerKernel = this.combinerKernelConstructor.newInstance(
              this, -1);
        } else {
          this.combinerKernel = null;
        }

        final HadoopCLKernel kernel;
        if (this.isMapper()) {
          this.thisKernelConstructor = this.mapperKernelConstructor;
          kernel = this.mapperKernel;
        } else if (this.isReducer()) {
          this.thisKernelConstructor = this.reducerKernelConstructor;
          kernel = this.reducerKernel;
        } else {
          this.thisKernelConstructor = this.combinerKernelConstructor;
          kernel = this.combinerKernel;
        }
        this.thisKernel = kernel;

        final Class<? extends HadoopCLInputBuffer> inputBufferClass =
          kernel.getInputBufferClass();
        final Class<? extends HadoopCLOutputBuffer> outputBufferClass =
          kernel.getOutputBufferClass();
        this.inputBufferConstructor = inputBufferClass.getConstructor(new Class[] {
          HadoopOpenCLContext.class, Integer.class });
        this.outputBufferConstructor = outputBufferClass.getConstructor(new Class[] {
          HadoopOpenCLContext.class, Integer.class });
      } catch(Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    private Device.TYPE retrieveCombinerDeviceType(Configuration conf) {
        final Device.TYPE combinerType;
        if (!conf.get(JobContext.OCL_COMBINER_DEVICE_TYPE, "FAIL").equals("FAIL")) {
          final String combinerTypeString = conf.get(
              JobContext.OCL_COMBINER_DEVICE_TYPE, "FAIL");
          EnumSet<Device.TYPE> allTypes = EnumSet.allOf(Device.TYPE.class);
          Device.TYPE result = null;
          for (Device.TYPE t : allTypes) {
            if (t.toString().equals(combinerTypeString)) {
              result = t;
              break;
            }
          }
          if (result == null) {
            combinerType = Device.TYPE.CPU;
          } else {
            combinerType = result;
          }
        } else {
          combinerType = Device.TYPE.CPU;
        }
        return combinerType;
    }

    private int findDeviceWithType(Device.TYPE type) {
        int devicesSoFar = 0;
        List<OpenCLPlatform> platforms = OpenCLUtil.getOpenCLPlatforms();
        for(OpenCLPlatform platform : platforms) {
            for(OpenCLDevice tmpDev : platform.getOpenCLDevices()) {
              if(tmpDev.getType() == type) {
                return devicesSoFar;
              }
              devicesSoFar++;
            }
        }
        return -1;
    }

    public static OpenCLDevice findDevice(int id) {
        int devicesSoFar = 0;
        OpenCLDevice dev = null;
        List<OpenCLPlatform> platforms = OpenCLUtil.getOpenCLPlatforms();
        for(OpenCLPlatform platform : platforms) {
            for(OpenCLDevice tmpDev : platform.getOpenCLDevices()) {
              if(devicesSoFar == id) {
                dev = tmpDev;
                break;
              }
              devicesSoFar++;
            }
            if(dev != null) break;
        }
        return dev;
    }

    public GlobalsWrapper getGlobals() { return this.globals; }
    public int[] getGlobalIndices() { return this.globals.globalIndices; }
    public double[] getGlobalsVal() { return this.globals.globalsVal; }
    public int[] getGlobalsInd() { return this.globals.globalsInd; }
    public double[] getGlobalsMapVal() { return this.globals.globalsMapVal; }
    public int[] getGlobalsMapInd() { return this.globals.globalsMapInd; }
    public int[] getGlobalsMap() { return this.globals.globalsMap; }
    public int nGlobalBuckets() { return this.globals.nGlobalBuckets; }
    public int getNGlobals() { return this.globals.nGlobals; }
    
    public boolean isCombiner() {
        return this.isCombiner;
    }

    public boolean isReducer() {
        return !isCombiner() && this.type.equals("reducer");
    }

    public boolean isMapper() {
        return !isCombiner() && this.type.equals("mapper");
    }

    public String verboseTypeName() {
        return this.verboseType;
    }

    public String typeName() {
        return this.type;
    }

    public int isGPU() {
        return this.isGPU;
    }

    public int getDeviceId() {
        return this.deviceId;
    }

    public int getDeviceSlot() {
        return this.deviceSlot;
    }

    public int getThreadsPerGroup() {
        return this.threadsPerGroup;
    }

    public OpenCLDevice getDevice() {
        return this.device;
    }

    public int getInputBufferSize() {
        return this.inputBufferSize;
    }

    public int getOutputBufferSize() {
        return this.outputBufferSize;
    }

    public int getPreallocIntLength() { return this.preallocIntLength; }
    public int getPreallocDoubleLength() { return this.preallocDoubleLength; }
    public int getPreallocFloatLength() { return this.preallocFloatLength; }
    public int getOutputBufferSpillChunk() { return this.outputBufferSpillChunk; }

    public String getDeviceString() {
        return this.deviceString;
    }

    public HadoopCLMapperKernel getMapperKernel() {
        return this.mapperKernel;
    }

    public HadoopCLReducerKernel getReducerKernel() {
        return this.reducerKernel;
    }

    public HadoopCLReducerKernel getCombinerKernel() {
        return this.combinerKernel;
    }

    public TaskInputOutputContext getContext() {
        return this.hadoopContext;
    }

    public boolean runningOnGPU() {
        return this.device != null &&
            this.device.getType() == Device.TYPE.GPU;
    }

    public boolean runningOnCPU() {
        return this.device != null &&
            this.device.getType() == Device.TYPE.CPU;
    }

    public int getNKernels() {
      return nKernels;
    }

    public int getNInputBuffers() {
      return nInputBuffers;
    }

    public int getNOutputBuffers() {
      return nOutputBuffers;
    }

    public boolean enableBufferRunnerDiagnostics() {
      return this.enableBufferRunnerDiagnostics;
    }

    public boolean enableProfilingPrints() {
      return this.enableProfilingPrints;
    }

    public boolean doHighLevelProfiling() {
      return this.doHighLevelProfiling;
    }

    public int getInputValMultiplier() {
      return this.inputValMultiplier;
    }

    public int getInputValEleMultiplier() {
        return this.inputValEleMultiplier;
    }

    // private HadoopCLKernel instantiateKernelObject(Class cls) {
    //     HadoopCLKernel kernel;
    //     try {
    //         kernel = (HadoopCLKernel)cls.newInstance();
    //     } catch (Exception e) {
    //         throw new RuntimeException(e);
    //     }
    //     return kernel;
    // }

    // public HadoopCLKernel newCombinerKernelObject() {
    //     return this.combinerKernel;
    // }

    public boolean jobHasCombiner() {
        return this.jobHasCombiner;
    }

    public int nCombinerKernels() {
        return this.nCombinerKernels;
    }

    public OpenCLDevice getCombinerDevice() {
        return findDevice(findDeviceWithType(retrieveCombinerDeviceType(
                this.hadoopContext.getConfiguration())));
    }
}
