package org.apache.hadoop.mapreduce;

public class RecordingDensity {
    private final double density;
    private final int device;

    public RecordingDensity(int setDevice, double setDensity) {
        this.density = setDensity;
        this.device = setDevice;
    }

    public int device() {
        return this.device;
    }

    public double density() {
        return this.density;
    }
}
