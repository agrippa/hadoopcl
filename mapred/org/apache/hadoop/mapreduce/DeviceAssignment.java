package org.apache.hadoop.mapreduce;

public class DeviceAssignment {
    private final int device;
    private final boolean speculative;

    public DeviceAssignment(int device, boolean speculative) {
        this.device = device;
        this.speculative = speculative;
    }

    public int device() {
        return this.device;
    }

    public boolean speculative() {
        return this.speculative;
    }

    @Override
    public String toString() {
        return "[device="+this.device+", speculative?="+this.speculative+"]";
    }
}
