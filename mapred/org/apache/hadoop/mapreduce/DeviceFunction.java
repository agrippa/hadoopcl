package org.apache.hadoop.mapreduce;

public class DeviceFunction {
    private double[] B;
    private double err;
    public DeviceFunction(double[] setB, double setErr) {
        this.B = setB;
        this.err = setErr;
    }

    public double[] B() {
        return this.B;
    }
    
    public double err() {
        return this.err;
    }
}
