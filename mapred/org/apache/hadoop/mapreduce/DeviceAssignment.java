package org.apache.hadoop.mapreduce;

public class DeviceAssignment {
    private final int device;
    private final int device_slot;
    private final boolean speculative;

    public DeviceAssignment(int device, int device_slot, boolean speculative) {
        this.device = device;
        this.device_slot = device_slot;
        this.speculative = speculative;
    }

    public int device() {
        return this.device;
    }

    public int device_slot() {
        return device_slot;
    }

    public boolean speculative() {
        return this.speculative;
    }

    @Override
    public String toString() {
        return "[device="+this.device+", device_slot="+this.device_slot+", speculative?="+this.speculative+"]";
    }
}
