
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

import org.apache.hadoop.io.SparseVectorWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopOpenCLContext {
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
    private final int nBuffs;
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

        String bucketsStr = System.getProperty("opencl.global.buckets");
        if (bucketsStr != null) {
          this.nGlobalBuckets = Integer.parseInt(bucketsStr);
        } else {
          this.nGlobalBuckets = 16;
        }

        String nBuffsStr = System.getProperty("opencl."+contextType+".buffers."+this.deviceString);
        if(nBuffsStr != null) {
            this.nBuffs = Integer.parseInt(nBuffsStr);
        } else {
            this.nBuffs = 3;
        }

        String vectorsToBufferStr = System.getProperty("opencl.vectorsToBuffer");
        if (vectorsToBufferStr != null) {
            this.nVectorsToBuffer = Integer.parseInt(vectorsToBufferStr);
        } else {
            this.nVectorsToBuffer = 65536;
        }

        List<SparseVectorWritable> bufferGlobals = new LinkedList<SparseVectorWritable>();
        int totalGlobals = 0;
        int countGlobals = 0;

        SequenceFile.Reader reader;
        try {
            reader = new SequenceFile.Reader(FileSystem.get(conf),
                    new Path(conf.get("opencl.properties.globalsfile")), conf);

            final IntWritable key = new IntWritable();
            SparseVectorWritable val = new SparseVectorWritable();
            while(reader.next(key, val)) {
                bufferGlobals.add(val);
                countGlobals++;
                totalGlobals += val.size();
                val = new SparseVectorWritable();
            }

            reader.close();

            final HashMap<Integer, List<IntDoublePair>> buckets =
              constructEmptyBuckets(this.nGlobalBuckets);
            this.globalIndices = new int[countGlobals];
            this.nGlobals = countGlobals;

            this.globalsInd = new int[totalGlobals];
            this.globalsVal = new double[totalGlobals];
            this.globalsFval = new float[totalGlobals];

            this.globalsMapInd = new int[totalGlobals];
            this.globalsMapVal = new double[totalGlobals];
            this.globalsMapFval = new float[totalGlobals];
            this.globalsMap = new int[this.nGlobalBuckets * countGlobals];

            int globalIndex = 0;
            int globalCount = 0;
            for(SparseVectorWritable g : bufferGlobals) {
                clearBuckets(buckets);
                this.globalIndices[globalCount] = globalIndex;
                for(int i = 0 ; i < g.size(); i++) {
                    buckets.get(g.indices()[i] % this.nGlobalBuckets).add(
                        new IntDoublePair(g.indices()[i], g.vals()[i]));
                    this.globalsInd[globalIndex] = g.indices()[i];
                    this.globalsVal[globalIndex] = g.vals()[i];
                    this.globalsFval[globalIndex] = (float)g.vals()[i];
                    globalIndex = globalIndex + 1;
                }

                int tmpGlobalIndex = this.globalIndices[globalCount];
                for (int bucket = 0; bucket < this.nGlobalBuckets; bucket++) {
                  this.globalsMap[globalCount * this.nGlobalBuckets + bucket] = tmpGlobalIndex;
                  // System.out.println("Global bucket "+(globalCount+bucket)+" at offset "+tmpGlobalIndex);
                  for (IntDoublePair element : buckets.get(bucket)) {
                    this.globalsMapInd[tmpGlobalIndex] = element.i;
                    this.globalsMapVal[tmpGlobalIndex] = element.d;
                    this.globalsMapFval[tmpGlobalIndex] = (float)element.d;
                    // System.out.print(this.globalsMapInd[tmpGlobalIndex]+":"+this.globalsMapVal[tmpGlobalIndex]+" ");
                    tmpGlobalIndex++;
                  }
                  // System.out.println();
                }

                globalCount = globalCount + 1;
            }
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

    private static class IntDoublePair {
      public final int i;
      public final double d;

      public IntDoublePair(int setI, double setD) {
        this.i = setI;
        this.d = setD;
      }
    }

    private HashMap<Integer, List<IntDoublePair>> constructEmptyBuckets(int nBuckets) {
      HashMap<Integer, List<IntDoublePair>> buckets = new HashMap<Integer, List<IntDoublePair>>();
      for (int i = 0 ; i < nBuckets; i++) {
        buckets.put(i, new LinkedList<IntDoublePair>());
      }
      return buckets;
    }

    private void clearBuckets(HashMap<Integer, List<IntDoublePair>> buckets) {
      for (Map.Entry<Integer, List<IntDoublePair>> entry : buckets.entrySet()) {
        entry.getValue().clear();
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

    public int getNBuffs() {
        return this.nBuffs;
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
