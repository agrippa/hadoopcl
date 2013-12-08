
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

public class HadoopOpenCLContext {
    public static final int GLOBAL_META_ID      = 0;
    public static final int GLOBAL_OFFSET_ID    = 1;
    public static final int GLOBAL_IND_ID       = 2;
    public static final int GLOBAL_VAL_ID       = 3;
    public static final int GLOBAL_FVAL_ID      = 4;
    public static final int GLOBAL_MAP_IND_ID   = 5;
    public static final int GLOBAL_MAP_VAL_ID   = 6;
    public static final int GLOBAL_MAP_FVAL_ID  = 7;
    public static final int GLOBAL_MAP_ID       = 8;

    private final TaskInputOutputContext hadoopContext;
    private final String type;
    private final int ngroups;
    private final int threadsPerGroup;
    private final OpenCLDevice device;
    private final int deviceId;
    private final int isGPU;
    private final Range r;
    private final int bufferSize;
    private final int preallocLength;
    private final String deviceString;
    private final int nVectorsToBuffer;

    private final int[] globalsInd;
    private final double[] globalsVal;
    private final float[] globalsFval;
    private final int[] globalIndices;
    private final int nGlobals;

    private final int[] globalsMapInd;
    private final double[] globalsMapVal;
    private final float[] globalsMapFval;
    private final int[] globalsMap;

    private final boolean isCombiner;

    private HadoopCLMapperKernel mapperKernel;
    private HadoopCLReducerKernel reducerKernel;
    private HadoopCLReducerKernel combinerKernel;

    private final int nGlobalBuckets;

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
        throw new RuntimeException("Failed to find CPU while searching for combiner device");
    }

    private OpenCLDevice findDevice(int id) {
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

    public HadoopOpenCLContext(String contextType, TaskInputOutputContext setHadoopContext) {
        this.hadoopContext = setHadoopContext;
        Configuration conf = this.hadoopContext.getConfiguration();

        if(contextType.equals("reducer") && this.hadoopContext.getTaskAttemptID().isMap()) {
            this.isCombiner = true;
            this.type = "combiner";
        } else {
            this.isCombiner = false;
            this.type = contextType;
        }

        if(this.isCombiner) {
            final Device.TYPE combinerType;
            if (System.getProperty("opencl.combiner.device") != null) {
              final String combinerTypeString =
                System.getProperty("opencl.combiner.device");
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

        String bufferSizeStr = System.getProperty("opencl."+contextType+".bufferSize."+this.deviceString);
        if(bufferSizeStr != null) {
            this.bufferSize = Integer.parseInt(bufferSizeStr);
        } else {
            this.bufferSize = 1048576;
        }

        String preallocLengthStr = System.getProperty("opencl.prealloc.length");
        if (preallocLengthStr != null) {
            this.preallocLength = Integer.parseInt(preallocLengthStr);
        } else {
            this.preallocLength = 1048576;
        }

        this.nGlobalBuckets = conf.getInt("opencl.global.buckets", 4096);

        String vectorsToBufferStr = System.getProperty("opencl.vectorsToBuffer");
        if (vectorsToBufferStr != null) {
            this.nVectorsToBuffer = Integer.parseInt(vectorsToBufferStr);
        } else {
            this.nVectorsToBuffer = 65536;
        }

        List<SparseVectorWritable> bufferGlobals =
            new LinkedList<SparseVectorWritable>();
        int totalGlobals = 0;
        int countGlobals = 0;

        SequenceFile.Reader reader;
        try {
            reader = new SequenceFile.Reader(FileSystem.get(conf),
                    new Path(conf.get("opencl.properties.globalsfile")), conf);

            final IntWritable key = new IntWritable();
            final ArrayPrimitiveWritable val = new ArrayPrimitiveWritable();
            forceNext(reader, key, val, HadoopOpenCLContext.GLOBAL_META_ID);
            int[] metadata = (int[])val.get();

            int countGlobals = metadata[0];
            int totalGlobals = metadata[1];

            this.nGlobals = countGlobals;
            forceNext(reader, key, val, HadoopOpenCLContext.GLOBAL_OFFSET_ID);
            this.globalIndices = (int[])val.get();

            forceNext(reader, key, val, HadoopOpenCLContext.GLOBAL_IND_ID);
            this.globalsInd = (int[])val.get();

            forceNext(reader, key, val, HadoopOpenCLContext.GLOBAL_VAL_ID);
            this.globalsVal = (double[])val.get();

            forceNext(reader, key, val, HadoopOpenCLContext.GLOBAL_FVAL_ID);
            this.globalsFval = (float[])val.get();

            forceNext(reader, key, val, HadoopOpenCLContext.GLOBAL_MAP_IND_ID);
            this.globalsMapInd = (int[])val.get();

            forceNext(reader, key, val, HadoopOpenCLContext.GLOBAL_MAP_VAL_ID);
            this.globalsMapVal = (double[])val.get();

            forceNext(reader, key, val, HadoopOpenCLContext.GLOBAL_MAP_FVAL_ID);
            this.globalsMapFval = (float[])val.get();

            forceNext(reader, key, val, HadoopOpenCLContext.GLOBAL_MAP_ID);
            this.globalsMap = (int[])val.get();

            reader.close();

        } catch(IOException io) {
            throw new RuntimeException(io);
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
                this.nGlobalBuckets);

            this.reducerKernel = (HadoopCLReducerKernel)reducerClass.newInstance();
            this.reducerKernel.init(this);
            this.reducerKernel.setGlobals(this.getGlobalsInd(),
                this.getGlobalsVal(), this.getGlobalsFval(),
                this.getGlobalIndices(), this.getNGlobals(),
                this.getGlobalsMapInd(), this.getGlobalsMapVal(),
                this.getGlobalsMapFval(), this.getGlobalsMap(),
                this.nGlobalBuckets);

            if(combinerClass != null) {
                this.combinerKernel = (HadoopCLReducerKernel)combinerClass.newInstance();
                this.combinerKernel.init(this);
                this.combinerKernel.setGlobals(this.getGlobalsInd(),
                    this.getGlobalsVal(), this.getGlobalsFval(),
                    this.getGlobalIndices(), this.getNGlobals(),
                    this.getGlobalsMapInd(), this.getGlobalsMapVal(),
                    this.getGlobalsMapFval(), this.getGlobalsMap(),
                    this.nGlobalBuckets);
            }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    private void forceNext(SequenceFile.Reader reader, IntWritable key,
            ArrayPrimitiveWritable val, int expected) throws IOException {
        if (reader.next(key, val) = false) {
            throw new RuntimeException("Unexpected next failure in forceNext");
        }
        if (expected != key.get()) {
            throw new RuntimeException("Expected vector id of "+expected+
                    " but got "+key.get());
        }
    }

    public int[] getGlobalIndices() { return this.globalIndices; }
    public double[] getGlobalsVal() { return this.globalsVal; }
    public float[] getGlobalsFval() { return this.globalsFval; }
    public int[] getGlobalsInd() { return this.globalsInd; }
    public double[] getGlobalsMapVal() { return this.globalsMapVal; }
    public float[] getGlobalsMapFval() { return this.globalsMapFval; }
    public int[] getGlobalsMapInd() { return this.globalsMapInd; }
    public int[] getGlobalsMap() { return this.globalsMap; }
    public int nGlobalBuckets() { return this.nGlobalBuckets; }
    public int getNGlobals() { return this.nGlobals; }
    
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

    public int getBufferSize() {
        return this.bufferSize;
    }

    public int getPreallocLength() {
        return this.preallocLength;
    }

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
}
