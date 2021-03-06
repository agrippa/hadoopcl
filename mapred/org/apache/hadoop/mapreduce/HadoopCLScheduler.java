package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus;

import java.lang.reflect.Constructor;
import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import java.io.IOException;
import java.util.jar.JarFile;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.net.URL;
import java.net.URLClassLoader;

import com.amd.aparapi.device.Device;
import com.amd.aparapi.internal.util.OpenCLUtil;
import com.amd.aparapi.internal.opencl.OpenCLPlatform;
import com.amd.aparapi.device.OpenCLDevice;
import com.amd.aparapi.device.Device.TYPE;

public abstract class HadoopCLScheduler {
    // List of available device types in a platform, indexed by device id
    protected final List<Device.TYPE> deviceTypes;
    // Current number of tasks placed on each device, does not account for
    // compute units of subdivided devices
    protected final int[] deviceOccupancy;
    // For each device that can be subdivided, the occupancy of those children
    // compute units
    protected final Map<Integer, int[]> subDeviceOccupancy;
    // Mapping from task attempt to device it is running on
    protected final ConcurrentHashMap<TaskAttemptID, DeviceLoad> taskToDevice;

    public abstract void removeTaskLoad(Task task, TaskStatus taskStatus, 
            JobConf conf, double[] avgOccupancy);
    public abstract DeviceAssignment bestCandidateDevice(Task task, JobConf conf) 
            throws IOException;

    public HadoopCLScheduler(JobConf conf) {
      this.subDeviceOccupancy = new HashMap<Integer, int[]>();
      this.deviceTypes = new ArrayList<Device.TYPE>();
      final List<OpenCLPlatform> platforms = OpenCLUtil.getOpenCLPlatforms();
      int platformId = 0;
      int deviceId = 0;
      for(OpenCLPlatform platform : platforms) {
          System.err.println("Platform "+platformId);
          for(OpenCLDevice device : platform.getOpenCLDevices()) {
              System.err.println("  Device "+deviceId+" "+device.getType().toString());
              deviceTypes.add(device.getType());
              if (device.getType() == Device.TYPE.CPU) {
                  this.subDeviceOccupancy.put(deviceId,
                          new int[device.getMaxComputeUnits()]);
              }
              deviceId++;
          }
          platformId++;
      }
      deviceTypes.add(Device.TYPE.JAVA); // special fake device

      this.deviceOccupancy = new int[deviceTypes.size()];
      for(int i = 0; i < deviceOccupancy.length; i++) {
          deviceOccupancy[i] = 0;
      }

      this.taskToDevice = new ConcurrentHashMap<TaskAttemptID, DeviceLoad>();
    }

    public int[] getOccupancyCopy() {
        int[] copy = new int[this.deviceOccupancy.length];
        System.arraycopy(this.deviceOccupancy, 0, copy, 0,
                this.deviceOccupancy.length);
        return copy;
    }

    public int numDevices() {
        return this.deviceTypes.size();
    }

    public List<Device.TYPE> getDeviceTypes() {
        return this.deviceTypes;
    }

    public int currentlyAssignedDevice(Task task) {
        return taskToDevice.get(task.getTaskID()).getDevice();
    }

    public String currentlyAssignedDeviceString(Task task) {
        return deviceTypeString(deviceTypes.get(taskToDevice.get(task.getTaskID()).getDevice()));
    }

    protected String deviceTypeString(Device.TYPE type) {
        switch(type) {
            case GPU:
                return "GPU";
            case CPU:
                return "CPU";
            case JTP:
                return "JTP";
            case SEQ:
                return "SEQ";
            case JAVA:
                return "JAVA";
        }
        return "UNKNOWN";
    }

    public static HadoopCLKernel getKernelForTask(Task task, JobConf conf) 
            throws IOException {
        String taskClassName = task.getMainClassName(conf);
        if(taskClassName == null) return null;

        // Construct class loader
        String jarStr = conf.getJar();
        JarFile jarFile = new JarFile(jarStr);
        Enumeration e = jarFile.entries();
        URL[] urls = { new URL("jar:file:"+jarStr+"!/") };
        ClassLoader cl = URLClassLoader.newInstance(urls);

        // Locate class for current task
        Class taskClass = null;
        while(e.hasMoreElements()) {
            JarEntry je = (JarEntry)e.nextElement();
            if(je.isDirectory() || !je.getName().endsWith(".class")) {
                continue;
            }
            String className = je.getName().substring(0, je.getName().length()-6);
            className = className.replace('/', '.');
            if(className.equals(taskClassName) ) {
                try {
                    taskClass = cl.loadClass(className);
                } catch(Exception ex) {
                    throw new RuntimeException("Exception loading URLClassLoader");
                }
                break;
            }
        }

        // Construct kernel object from this task's main class
        HadoopCLKernel kernel = null;
        try {
            Constructor<? extends HadoopCLKernel> c = taskClass.getConstructor(
                new Class[] { HadoopOpenCLContext.class, Integer.class });
            kernel = c.newInstance(new HadoopOpenCLContext(), -1);
        } catch(Exception ex) {
            throw new RuntimeException("Exception loading kernel class in TaskTracker ("+jarStr+" | "+taskClassName+")", ex);
        }
        return kernel;
    }
}
