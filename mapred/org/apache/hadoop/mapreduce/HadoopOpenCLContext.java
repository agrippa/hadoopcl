
package org.apache.hadoop.mapreduce;

import java.util.Map;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.List;
import com.amd.aparapi.Range;
import com.amd.aparapi.internal.opencl.OpenCLPlatform;
import com.amd.aparapi.device.OpenCLDevice;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.internal.util.OpenCLUtil;
import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;

import org.apache.hadoop.io.SparseVectorWritable;

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
    private final String deviceString;
    private final int nBuffs;
    private final int nVectorsToBuffer;

    private final int[] globalsInd;
    private final double[] globalsVal;
    private final int[] globalIndices;
    private final int nGlobals;
    private final boolean isCombiner;

    private HadoopCLMapperKernel mapperKernel;
    private HadoopCLReducerKernel reducerKernel;
    private HadoopCLReducerKernel combinerKernel;

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
            this.deviceId = -1;
        } else if(System.getProperty("opencl.device") != null) {
            this.deviceId = Integer.parseInt(System.getProperty("opencl.device"));
        } else {
            this.deviceId = 0;
        }


        int devicesSoFar = 0;
        OpenCLDevice dev = null;
        List<OpenCLPlatform> platforms = OpenCLUtil.getOpenCLPlatforms();
        for(OpenCLPlatform platform : platforms) {
            for(OpenCLDevice tmpDev : platform.getOpenCLDevices()) {
                if(devicesSoFar == this.deviceId) {
                    dev = tmpDev;
                    break;
                }
                devicesSoFar++;
            }
            if(dev != null) break;
        }
        this.device = dev;

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
        Iterator<SparseVectorWritable> globalIter = conf.getHadoopCLGlobalsIterator();
        int totalGlobals = 0;
        int countGlobals = 0;
        while(globalIter.hasNext()) {
            SparseVectorWritable g = globalIter.next();
            countGlobals++;
            totalGlobals += g.size();
            bufferGlobals.add(g);
        }

        this.globalsInd = new int[totalGlobals];
        this.globalsVal = new double[totalGlobals];
        this.globalIndices = new int[countGlobals];
        this.nGlobals = countGlobals;

        int globalIndex = 0;
        int globalCount = 0;
        for(SparseVectorWritable g : bufferGlobals) {
            this.globalIndices[globalCount] = globalIndex;
            for(int i = 0 ; i < g.size(); i++) {
                this.globalsInd[globalIndex] = g.indices()[i];
                this.globalsVal[globalIndex] = g.vals()[i];
                globalIndex = globalIndex + 1;
            }
            globalCount = globalCount + 1;
        }

        try {
            Class mapperClass = hadoopContext.getOCLMapperClass();
            Class reducerClass = hadoopContext.getOCLReducerClass();
            Class combinerClass = hadoopContext.getOCLCombinerClass();

            this.mapperKernel = (HadoopCLMapperKernel)mapperClass.newInstance();
            this.mapperKernel.init(this);
            this.mapperKernel.setGlobals(this.getGlobalsInd(), this.getGlobalsVal(),
                this.getGlobalIndices(), this.getNGlobals());

            this.reducerKernel = (HadoopCLReducerKernel)reducerClass.newInstance();
            this.reducerKernel.init(this);
            this.reducerKernel.setGlobals(this.getGlobalsInd(), this.getGlobalsVal(),
                this.getGlobalIndices(), this.getNGlobals());

            if(combinerClass != null) {
                this.combinerKernel = (HadoopCLReducerKernel)combinerClass.newInstance();
                this.combinerKernel.init(this);
                this.combinerKernel.setGlobals(this.getGlobalsInd(), this.getGlobalsVal(),
                        this.getGlobalIndices(), this.getNGlobals());
            }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    public int[] getGlobalIndices() {
        return this.globalIndices;
    }

    public double[] getGlobalsVal() {
        return this.globalsVal;
    }

    public int[] getGlobalsInd() {
        return this.globalsInd;
    }

    public int getNGlobals() {
        return this.nGlobals;
    }
    
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
