package org.apache.hadoop.mapreduce;

import com.amd.aparapi.device.Device;
import java.io.IOException;

public class KernelThreadDone extends HadoopCLKernel {

    public KernelThreadDone(HadoopOpenCLContext clContext, Integer id) {
        super(clContext, id);
    }

    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return null; }
    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return null; }
    public boolean launchKernel() throws IOException, InterruptedException { return true; }
    public boolean relaunchKernel() throws IOException, InterruptedException { return true; }
    public IHadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException { return null; }
    public void fill(HadoopCLInputBuffer inputBuffer) { }
    public void prepareForRead(HadoopCLOutputBuffer outputBuffer) { }

    public void deviceStrength(DeviceStrength str) { }
    public Device.TYPE[] validDevices() { return null; }
    public boolean equalInputOutputTypes() { return true; }

    public void run() { }
}
