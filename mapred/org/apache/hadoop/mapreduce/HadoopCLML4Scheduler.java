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
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.jar.JarFile;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.net.URL;
import java.net.URLClassLoader;

public class HadoopCLML4Scheduler extends HadoopCLPredictiveScheduler<Device.TYPE, Double> {

    public HadoopCLML4Scheduler(JobConf conf) {
      super(conf);
      loadFromDirectory(conf.get("opencl.recordings_folder"));

      //for(String taskName : taskPerfProfile.keySet()) {
      //    AHadoopCLTaskCharacterization profile = taskPerfProfile.get(taskName);
      //    profile.removeOutliers();
      //}
    }

    public AHadoopCLTaskCharacterization<Device.TYPE, Double> getCharacterizationObject(String taskName, 
            List<Device.TYPE> deviceTypes, boolean isMapper) {
        return new HadoopCLTaskCharacterization4(taskName, deviceTypes.size(), deviceTypes, isMapper);
    }
    public HadoopCLRecording<Double> getRecordingObject(int device, double rate, double[] occ) {
        return new HadoopCLDoubleRecording(device, rate, occ);
    }
    public Device.TYPE getMappingObject(int device) {
        return this.deviceTypes.get(device);
    }
    @Override
    public boolean recordLaunches() {
        return true;
    }
	
	@Override
	public int shouldSwitchPlatform(Task task, JobConf conf, 
			long expectedRemainingTime, long expectedTotalInputs) throws IOException {
        int currentDevice = this.currentlyAssignedDevice(task);

        String taskClassName = task.getMainClassName(conf);
        AHadoopCLTaskCharacterization<Device.TYPE, Double> taskProfile = this.getCharacterizationObject(taskClassName, 
                this.deviceTypes, task.isMapTask());
        taskProfile = taskPerfProfile.putIfAbsent(taskClassName, taskProfile);
        if(taskProfile == null) taskProfile = taskPerfProfile.get(taskClassName);

		long best = (long)((double)expectedRemainingTime * 0.9);
		int bestDevice = -1;
		
        synchronized(this.deviceOccupancy) {
            for(int device = 0; device < this.deviceOccupancy.length; device++) {
                int occ;
                occ = this.deviceOccupancy[device];
                if(device == currentDevice) occ--;
                double expected = taskProfile.predict(this.deviceTypes.get(device), (double)occ);

                if(expected <= 0.0) continue;

                long expectedTime = (long)((double)expectedTotalInputs / expected);
                if(expectedTime < best) {
                    best = expectedTime;
                    bestDevice = device;
                }
            }

            if(bestDevice != -1) {
                int minOcc = -1;
                int minOccDevice = -1;

                Device.TYPE t = this.deviceTypes.get(bestDevice);
                for(int i = 0; i < this.deviceOccupancy.length; i++) {
                    if(this.deviceTypes.get(i) == t) {
                        if(minOccDevice == -1 || this.deviceOccupancy[i] < minOcc) {
                            minOcc = this.deviceOccupancy[i];
                            minOccDevice = i;
                        }
                    }
                }
                bestDevice = minOccDevice;
            }
        }
		
		return bestDevice;
	}

    @Override
    public void removeTaskLoad(Task task, TaskStatus taskStatus,
            JobConf conf, double[] avgOccupancy) {
        DeviceLoad load = taskToDevice.remove(task.getTaskID());
        if(load != null) { // just in case?
            String taskClassName = task.getMainClassName(conf);
            if(taskStatus.getNInputs() > 0) {
                double processingRate = (double)taskStatus.getNInputs() / (double)taskStatus.getProcessingTime();

                AHadoopCLTaskCharacterization<Device.TYPE, Double> taskProfile = this.getCharacterizationObject(taskClassName, 
                        this.deviceTypes, task.isMapTask());
                taskProfile = taskPerfProfile.putIfAbsent(taskClassName, taskProfile);
                if(taskProfile == null) taskProfile = taskPerfProfile.get(taskClassName);

                int device = load.getDevice();
                taskProfile.addRecording(this.getMappingObject(load.getDevice()), 
                    this.getRecordingObject(load.getDevice(), processingRate, avgOccupancy));
                writeRecording(taskClassName, load.getDevice(), 
                        processingRate, avgOccupancy,
                        task.isMapTask(),
                        taskProfile.getPredictor(this.deviceTypes.get(load.getDevice())));
            }

            synchronized(this.deviceOccupancy) {
                this.deviceOccupancy[load.getDevice()]--;
            }

        }
    }
}
