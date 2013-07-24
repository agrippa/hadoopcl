package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus;
import java.io.IOException;
import com.amd.aparapi.device.Device;

public class HadoopCLDeviceChecker {
    private final Task task;
    private final JobConf conf;
    private final AHadoopCLTaskCharacterization profile;
    private final HadoopCLScheduler scheduler;

    public HadoopCLDeviceChecker(Task t, JobConf conf, 
            AHadoopCLTaskCharacterization<?,?> profile,
            HadoopCLScheduler scheduler) {
        this.task = t;
        this.conf = conf;
        this.profile = profile;
        this.scheduler = scheduler;
    }

    public boolean validDeviceForTask(int device) throws IOException {
        return validDeviceForTask(scheduler.getDeviceTypes().get(device));
    }

    public boolean validDeviceForTask(Device.TYPE type) throws IOException {
        HadoopCLKernel kernel = profile.getKernelObject(this.task, this.conf);
        Device.TYPE[] valid = kernel.validDevices();

        if(valid == null) return true; // all are valid

        for(Device.TYPE v : valid) {
            if(v == type) {
                return true;
            }
        }
        return false;

    }
}
