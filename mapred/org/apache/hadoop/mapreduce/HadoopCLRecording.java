package org.apache.hadoop.mapreduce;

import java.util.List;

public abstract class HadoopCLRecording<RecordingType> 
        implements Comparable<HadoopCLRecording<RecordingType>>{
    private final double[] originalOccupancy;
    protected double rate;
    protected RecordingType deviceOccupancy;

    public HadoopCLRecording(int device, double rate, double[] setOccupancy) {
        this.rate = rate;
        this.originalOccupancy = setOccupancy;
    }

    public HadoopCLRecording(List<String> tokens) {
        this.originalOccupancy = null;
        this.deviceOccupancy = null;
        deserialize(tokens);
    }

    public double getRate() {
        return this.rate;
    }

    public RecordingType getOccupancy() {
        return deviceOccupancy;
    }

    public double[] fullOccupancy() {
        return this.originalOccupancy;
    }

    public double distance(int[] other) {
        double sum = 0.0;
        for(int i = 0; i < this.originalOccupancy.length; i++) {
            double diff = this.originalOccupancy[i] - other[i];
            sum = sum + (diff * diff);
        }
        return Math.sqrt(sum);
    }

    public abstract double distance(HadoopCLRecording<RecordingType> other);
    public abstract double distance(RecordingType other);
    public abstract HadoopCLRecording<RecordingType> getOrigin();
    public abstract HadoopCLRecording<RecordingType> mean(List<HadoopCLRecording<RecordingType>> recordings);
    public abstract List<String> serialize();
    protected abstract void deserialize(List<String> tokens);

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{ [ ");
        for(double d : originalOccupancy) {
            sb.append(d);
            sb.append(" ");
        }
        sb.append("] -> ");
        sb.append(this.rate);
        sb.append(" }");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return (int)this.rate;
    }
}

