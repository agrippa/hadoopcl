
package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.ArrayList;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.device.Device.TYPE;

public class ProfileContext {
    private static final boolean PROFILE = false;

    private long kernelWaitTime = 0;
    private List<Long> kernelWaits = new ArrayList<Long>();
    private long outputsFromOpenCLTime = 0;
    private List<Long> fromOpenCL = new ArrayList<Long>();
    private long inputsToHadoop = 0;
    private List<Long> toHadoop = new ArrayList<Long>();
    private List<Integer> launches = new ArrayList<Integer>();

    private List<Long> putTimes = new ArrayList<Long>();
    private long putTimeTotal = 0;
    private List<Long> executeTimes = new ArrayList<Long>();
    private long executeTimeTotal = 0;

    private final String type;

    private long start, stop;

    private long startTime = 0;
    private long stopTime = 0;

    private final HadoopOpenCLContext clContext;

    private final String deviceStr;

    public ProfileContext(String setType, HadoopOpenCLContext clContext) {
        this.type = setType;
        this.clContext = clContext;

        if(this.clContext.getDevice() == null) {
            this.deviceStr = "JAVA";
        } else if (this.clContext.getDevice().getType() == TYPE.GPU) {
            this.deviceStr = "GPU";
        } else if (this.clContext.getDevice().getType() == TYPE.CPU) {
            this.deviceStr = "CPU";
        } else {
            this.deviceStr = "UNKNOWN";
        }


        //System.out.println("Starting "+this.type+" task on device "+this.clContext.getDeviceId()+", "+this.deviceStr);
    }

    public void startOverallTimer() {
        start = System.currentTimeMillis();
    }

    public void stopOverallTimer() {
        stop = System.currentTimeMillis();
    }

    public void startTimer() {
        startTime = System.currentTimeMillis();
    }

    private void stopTimer() {
        stopTime = System.currentTimeMillis();
    }

    public void stopKernelWaitsTimer() {
        if(PROFILE) {
            stopTimer();
            kernelWaits.add(new Long(stopTime - startTime));
            kernelWaitTime += (stopTime - startTime);
        }
    }

    public void stopOutputsFromOpenCLTimer() {
        if(PROFILE) {
            stopTimer();
            fromOpenCL.add(new Long(stopTime-startTime));
            outputsFromOpenCLTime += (stopTime - startTime);
        }
    }

    public void stopToHadoopTimer() {
        stopTimer();
        toHadoop.add(new Long(stopTime-startTime));
        inputsToHadoop += (stopTime - startTime);
    }

    public void stopPutsTimer() {
        if(PROFILE) {
            stopTimer();
            putTimes.add(new Long(stopTime-startTime));
            putTimeTotal += (stopTime-startTime);
        }
    }

    public void stopExecuteTimer() {
        if(PROFILE) {
            stopTimer();
            executeTimes.add(new Long(stopTime-startTime));
            executeTimeTotal += (stopTime-startTime);
        }
    }

    public void addLaunch(int size) {
        launches.add(size);
    }

    public boolean profilingEnabled() {
        return PROFILE;
    }

    public String toString(long nInputPairs) {

        if(!PROFILE) {

            String str = "DIAGNOSTICS: Profiling not enabled (device "+this.clContext.getDeviceId()+", "+this.deviceStr+", "+this.type;
            str += (", inputs to hadoop time "+inputsToHadoop+"(");
            for(Long l : toHadoop) str += (l.toString()+" ");
            str += "), launches (";
            for(Integer i : launches) str += (i.toString()+" ");
            str = str + "), Total time: "+(stop-start)+" ("+stop+"->"+start+")";
            return str;
        }

        String str = "";
        str += "DIAGNOSTICS: "+this.type+"(";
        str += this.clContext.getDeviceString()+"(device "+this.clContext.getDeviceId()+"), "+
            this.clContext.getNBuffs()+" buffers, "+this.clContext.getBufferSize()+" bytes per buffer, "+
            this.clContext.getNGroups()+" thread groups, "+this.clContext.getThreadsPerGroup()+" threads per group";
        str += "): Total time: "+(stop-start)+" for "+nInputPairs+" input pairs";
        str += ", kernel wait time "+kernelWaitTime+"(";
        for(Long l : kernelWaits) str += (l.toString()+" ");
        str += ("), retrieve output from OpenCL time "+outputsFromOpenCLTime+"(");
        for(Long l : fromOpenCL) str += (l.toString()+" ");
        str += ("), inputs to hadoop time "+inputsToHadoop+"(");
        for(Long l : toHadoop) str += (l.toString()+" ");
        str += ("), puts time "+putTimeTotal+"(");
        for(Long l : putTimes) str += (l.toString()+" ");
        str += ("), execute time "+executeTimeTotal+"(");
        for(Long l : executeTimes) str += (l.toString()+" ");
        str += ("), "+launches.size()+" launches(");
        for(Integer i : launches) str += (i.toString()+" ");
        str += ("), "+System.getProperty("opencl."+this.type+".device"));
        return str;
    }
}
