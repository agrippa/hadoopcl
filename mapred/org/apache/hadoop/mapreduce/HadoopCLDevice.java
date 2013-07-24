package org.apache.hadoop.mapreduce;

import com.amd.aparapi.device.Device;

public class HadoopCLDevice {
    private final Device.TYPE type;
    private final int id;

    public HadoopCLDevice(int id, Device.TYPE type) {
        this.id = id;
        this.type = type;
    }

    public int id() {
        return this.id;
    }
    public Device.TYPE type() {
        return this.type;
    }
}
