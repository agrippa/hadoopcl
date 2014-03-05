package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus;

import com.amd.aparapi.internal.util.OpenCLUtil;
import com.amd.aparapi.internal.opencl.OpenCLPlatform;
import com.amd.aparapi.device.OpenCLDevice;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.device.Device.TYPE;

import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;
import java.util.jar.JarFile;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.net.URL;
import java.net.URLClassLoader;

public class HadoopCLBalanceScheduler extends HadoopCLScheduler {
    private double[] deviceLoads;

    public HadoopCLBalanceScheduler() {
      deviceLoads = new double[deviceTypes.size()];
      for(int i = 0; i < deviceLoads.length; i++) {
          deviceLoads[i] = 0.0;
      }
    }

    public DeviceAssignment bestCandidateDevice(Task task, JobConf conf) 
            throws IOException {
        HadoopCLKernel kernel = HadoopCLScheduler.getKernelForTask(task, conf);
        if(kernel == null) {
            return new DeviceAssignment(deviceOccupancy.length-1, -1, false); // running in Java
        }

        DeviceStrength strengths = new DeviceStrength();
        kernel.deviceStrength(strengths);

        int optimalDevice = -1;
        int optimalSlot = -1;
        double minLoad = -1.0;
        double addedLoad = -1.0;

        synchronized(this.deviceOccupancy) {
            for(int i = 0; i < deviceLoads.length; i++) {
                int strength = strengths.get(deviceTypes.get(i));
                if(strength > 0) {
                    double load = 1.0 / (double)strength;
                    double newLoad = deviceLoads[i] + load;
                    if(optimalDevice == -1 || newLoad < minLoad) {
                        optimalDevice = i;
                        minLoad = newLoad;
                        addedLoad = load;
                    }
                }
            }
            if(optimalDevice == -1) {
                throw new RuntimeException("Unable to find any devices for task \""+task.toString()+"\". Whoops... How?");
            }
            this.deviceLoads[optimalDevice] = minLoad;
            this.deviceOccupancy[optimalDevice]++;

            int[] subdevices = this.subDeviceOccupancy.get(optimalDevice);
            if (subdevices != null) {
                synchronized (subdevices) {
                    minLoad = subdevices[0];
                    int minLoadIndex = 0;
                    for (int i = 1; i < subdevices.length; i++) {
                        if (subdevices[i] < minLoad) {
                            minLoad = subdevices[i];
                            minLoadIndex = i;
                        }
                    }
                    optimalSlot = minLoadIndex;
                    subdevices[minLoadIndex]++;
                }
            }

            taskToDevice.put(task.getTaskID(), new DeviceLoad(optimalDevice,
                  optimalSlot, addedLoad, this.deviceOccupancy));
        }

        return new DeviceAssignment(optimalDevice, optimalSlot, false);
    }

    public void removeTaskLoad(Task task, TaskStatus taskStatus,
            JobConf conf, double[] avgOccupancy) {
        DeviceLoad load = taskToDevice.remove(task.getTaskID());
        if(load != null) { // just in case?
            synchronized(this.deviceOccupancy) {
                deviceLoads[load.getDevice()] -= load.getLoad();
                deviceOccupancy[load.getDevice()]--;
                int[] subdevices = this.subDeviceOccupancy.get(load.getDevice());
                if (subdevices != null) {
                    synchronized (subdevices) {
                        subdevices[load.getDeviceSlot()]--;
                    }
                }
            }
        }
    }
}
