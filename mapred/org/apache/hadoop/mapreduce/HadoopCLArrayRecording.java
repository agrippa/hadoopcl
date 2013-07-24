package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.ArrayList;

public class HadoopCLArrayRecording extends HadoopCLRecording<double[]> {
    public HadoopCLArrayRecording(int device, double rate, double[] setOccupancy) {
        super(device, rate, setOccupancy);
        this.deviceOccupancy = setOccupancy;
    }

    public HadoopCLArrayRecording(List<String> tokens) {
        super(tokens);
    }

    public double distance(HadoopCLRecording<double[]> other) {
        double sum = 0.0;
        for(int i = 0; i < this.deviceOccupancy.length; i++) {
            double diff = this.deviceOccupancy[i] - other.deviceOccupancy[i];
            sum = sum + (diff * diff);
        }
        double diff = other.rate - this.rate;
        sum = sum + (diff*diff);
        return Math.sqrt(sum);
    }

    @Override
    public double distance(double[] other) {
        double sum = 0.0;
        for(int i = 0; i < this.deviceOccupancy.length; i++) {
            double diff = this.deviceOccupancy[i] - other[i];
            sum = sum + (diff * diff);
        }
        return Math.sqrt(sum);
    }

    public HadoopCLRecording<double[]> getOrigin() {
        double[] origin = new double[this.deviceOccupancy.length];
        for(int i = 0; i < origin.length; i++) {
            origin[i] = 0.0;
        }
        return new HadoopCLArrayRecording(0, 0.0, origin);
    }

    public HadoopCLRecording<double[]> mean(List<HadoopCLRecording<double[]>> recordings) {
        double[] sum = new double[this.deviceOccupancy.length];
        double sumRate = 0.0;
        for(HadoopCLRecording<double[]> r : recordings) {
            for(int i = 0 ; i < sum.length; i++) {
                sum[i] += r.deviceOccupancy[i];
            }
            sumRate = sumRate + r.getRate();
        }
        for(int i = 0; i < sum.length; i++) {
            sum[i] = sum[i] / (double)recordings.size();
        }
        sumRate = sumRate / (double)recordings.size();
        return new HadoopCLArrayRecording(-1, sumRate, sum);
    }

    @Override
    public List<String> serialize() {
        List<String> tokens = new ArrayList<String>();
        tokens.add(Double.toString(this.rate));
        for(double d : this.deviceOccupancy) {
            tokens.add(Double.toString(d));
        }
        return tokens;
    }

    @Override
    public void deserialize(List<String> tokens) {
        this.rate = Double.parseDouble(tokens.get(0));
        this.deviceOccupancy = new double[tokens.size()-1];
        for(int i = 1; i < tokens.size(); i++) {
            this.deviceOccupancy[i-1] = Double.parseDouble(tokens.get(i));
        }
    }

    @Override
    public int compareTo(HadoopCLRecording<double[]> other) {
        double[] me = this.deviceOccupancy;
        double[] them = other.deviceOccupancy;

        double meDist = 0.0;
        double themDist = 0.0;

        for(int i = 0; i < me.length; i++) {
            meDist = meDist + (me[i] * me[i]);
            themDist = themDist + (me[i] * me[i]);
        }

        if(meDist < themDist) {
            return -1;
        } else if (meDist > themDist) {
            return 1;
        } else {
            return 0;
        }
    }
}
