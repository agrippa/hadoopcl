
package org.apache.hadoop.mapreduce;

import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.List;
import java.util.EnumSet;
import com.amd.aparapi.Range;
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
    private int ngroups;
    private int threadsPerGroup;
    private OpenCLDevice device;
    private int deviceId;
    private int isGPU;
    private Range r;
    private final int inputBufferSize;
    private final int outputBufferSize;
    private final int inputValMultiplier;
    private final int preallocIntLength;
    private final int preallocFloatLength;
    private final int preallocDoubleLength;
    private String deviceString;
    private int nVectorsToBuffer;

    private boolean isCombiner;

    private HadoopCLMapperKernel mapperKernel;
    private HadoopCLReducerKernel reducerKernel;
    private HadoopCLReducerKernel combinerKernel;

    private GlobalsWrapper globals;
    
    private final int nKernels;
    private final int nInputBuffers;
    private final int nOutputBuffers;

    public HadoopOpenCLContext(String contextType,
        TaskInputOutputContext setHadoopContext, GlobalsWrapper globals) {

      this.hadoopContext = setHadoopContext;

      if (this.hadoopContext.getContextType() == ContextType.Combiner) {
          this.isCombiner = true;
          this.type = "combiner";
      } else {
          this.isCombiner = false;
          this.type = contextType;
      }

      Configuration conf = this.hadoopContext.getConfiguration();
      this.nKernels = conf.getInt("opencl."+type+".nkernels", 1);
      this.nInputBuffers = conf.getInt("opencl."+type+".ninputbuffers", 3);
      this.nOutputBuffers = conf.getInt("opencl."+type+".noutputbuffers", 1);
      this.preallocIntLength = conf.getInt("opencl.prealloc.length.int", 5242880);
      this.preallocFloatLength = conf.getInt("opencl.prealloc.length.float", 5242880);
      this.preallocDoubleLength = conf.getInt("opencl.prealloc.length.double", 5242880);
      this.enableBufferRunnerDiagnostics = conf.getBoolean("opencl.buffer.diagnostics", false);
      this.enableProfilingPrints = conf.getBoolean("opencl.profiling", false);
      this.doHighLevelProfiling = conf.getBoolean("opencl.highlevel", false);
      this.inputBufferSize = conf.getInt("opencl."+this.type+".inputBufferSize", 32768);
      this.outputBufferSize = conf.getInt("opencl."+this.type+".outputBufferSize", 32768);
      this.inputValMultipler = conf.getInt("opencl."+this.type+".val_multiplier", 5);

      init(contextType, conf, globals);
    }

    public void init(String contextType, Configuration conf,
            GlobalsWrapper globals) {

        this.globals = globals;
        synchronized(this.globals) {
          this.globals.init(conf);
        }

        if (this.isCombiner) {
            final Device.TYPE combinerType;
            if (!conf.get(JobContext.OCL_COMBINER_DEVICE_TYPE, "FAIL").equals("FAIL")) {
              final String combinerTypeString = conf.get(JobContext.OCL_COMBINER_DEVICE_TYPE, "FAIL");
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

            this.deviceId = findDeviceWithType(combinerType);
            // this.deviceId = -1;
            // this.deviceId = Integer.parseInt(System.getProperty("opencl.device"));
        } else if(System.getProperty("opencl.device") != null) {
            this.deviceId = Integer.parseInt(System.getProperty("opencl.device"));
        } else {
            this.deviceId = 0;
        }
        this.device = findDevice(this.deviceId);

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
       
        String groupsStr = System.getProperty("opencl."+contextType+".groups."+this.deviceString);
        if(groupsStr != null) {
            this.ngroups = Integer.parseInt(groupsStr);
        } else {
            this.ngroups = 32;
        }

        String threadsPerGroupStr = System.getProperty("opencl."+contextType+".threadsPerGroup."+this.deviceString);
        if(threadsPerGroupStr != null) {
            this.threadsPerGroup = Integer.parseInt(threadsPerGroupStr);
        } else {
            this.threadsPerGroup = 256;
        }

        if(getDevice() == null) {
            this.r = null;
        } else {
            this.r = getDevice().createRange(getNGroups() * getThreadsPerGroup(), getThreadsPerGroup());
        }

        String vectorsToBufferStr = System.getProperty("opencl.vectorsToBuffer");
        if (vectorsToBufferStr != null) {
            this.nVectorsToBuffer = Integer.parseInt(vectorsToBufferStr);
        } else {
            this.nVectorsToBuffer = 65536;
        }

        try {
            Class mapperClass = hadoopContext.getOCLMapperClass();
            Class reducerClass = hadoopContext.getOCLReducerClass();
            Class combinerClass = hadoopContext.getOCLCombinerClass();

            this.mapperKernel = (HadoopCLMapperKernel)mapperClass.newInstance();
            this.mapperKernel.init(this);
            this.mapperKernel.setGlobals(this.getGlobalsInd(),
                this.getGlobalsVal(), this.getGlobalsFval(),
                this.getGlobalIndices(), this.getNGlobals(),
                this.getGlobalsMapInd(), this.getGlobalsMapVal(),
                this.getGlobalsMapFval(), this.getGlobalsMap(),
                this.globals.nGlobalBuckets);

            this.reducerKernel = (HadoopCLReducerKernel)reducerClass.newInstance();
            this.reducerKernel.init(this);
            this.reducerKernel.setGlobals(this.getGlobalsInd(),
                this.getGlobalsVal(), this.getGlobalsFval(),
                this.getGlobalIndices(), this.getNGlobals(),
                this.getGlobalsMapInd(), this.getGlobalsMapVal(),
                this.getGlobalsMapFval(), this.getGlobalsMap(),
                this.globals.nGlobalBuckets);

            if(combinerClass != null) {
                this.combinerKernel = (HadoopCLReducerKernel)combinerClass.newInstance();
                this.combinerKernel.init(this);
                this.combinerKernel.setGlobals(this.getGlobalsInd(),
                    this.getGlobalsVal(), this.getGlobalsFval(),
                    this.getGlobalIndices(), this.getNGlobals(),
                    this.getGlobalsMapInd(), this.getGlobalsMapVal(),
                    this.getGlobalsMapFval(), this.getGlobalsMap(),
                    this.globals.nGlobalBuckets);
            }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }

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

    public int[] getGlobalIndices() { return this.globals.globalIndices; }
    public double[] getGlobalsVal() { return this.globals.globalsVal; }
    public float[] getGlobalsFval() { return this.globals.globalsFval; }
    public int[] getGlobalsInd() { return this.globals.globalsInd; }
    public double[] getGlobalsMapVal() { return this.globals.globalsMapVal; }
    public float[] getGlobalsMapFval() { return this.globals.globalsMapFval; }
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

    public String typeName() {
        return this.type;
    }

    public int getNVectorsToBuffer() {
        return this.nVectorsToBuffer;
    }

    public int isGPU() {
        return this.isGPU;
    }

    public int getDeviceId() {
        return this.deviceId;
    }

    public int getNGroups() {
        return this.ngroups;
    }

    public int getThreadsPerGroup() {
        return this.threadsPerGroup;
    }

    public OpenCLDevice getDevice() {
        return this.device;
    }

    public Range getRange() {
        return this.r;
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
      return this.inputValMultipler;
    }
}
