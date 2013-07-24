package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.ArrayList;

public class HadoopCLDoubleRecording extends HadoopCLRecording<Double> {
    public HadoopCLDoubleRecording(int device, double rate, double[] setOccupancy) {
        super(device, rate, setOccupancy);
        this.deviceOccupancy = new Double(setOccupancy[device]);
    }

    public HadoopCLDoubleRecording(int device, double rate, double occupancy) {
        super(device, rate, null);
        this.deviceOccupancy = new Double(occupancy);
    }

    public HadoopCLDoubleRecording(List<String> tokens) {
        super(tokens);
    }

    @Override
    public double distance(HadoopCLRecording<Double> other) {
        double occDiff = other.deviceOccupancy.doubleValue() - 
            this.deviceOccupancy.doubleValue();
        double rateDiff = other.rate - this.rate;
        return Math.sqrt(occDiff*occDiff + rateDiff*rateDiff);
    }

    @Override
    public double distance(Double other) {
        return Math.abs(this.deviceOccupancy.doubleValue() - 
                other.doubleValue());
    }

    public HadoopCLRecording<Double> getOrigin() {
        return new HadoopCLDoubleRecording(0, 0.0, new double[] { 0.0 });
    }

    public HadoopCLRecording<Double> mean(List<HadoopCLRecording<Double>> recordings) {
        double sumOccupancy = 0.0;
        double sumRate = 0.0;
        for(HadoopCLRecording<Double> r : recordings) {
            sumOccupancy = sumOccupancy + r.deviceOccupancy;
            sumRate = sumRate + r.getRate();
        }
        sumOccupancy = sumOccupancy / (double)recordings.size();
        sumRate = sumRate / (double)recordings.size();
        return new HadoopCLDoubleRecording(-1, sumRate, sumOccupancy);
    }

    @Override
    public List<String> serialize() {
        List<String> tokens = new ArrayList<String>();
        tokens.add(Double.toString(this.rate));
        tokens.add(this.deviceOccupancy.toString());
        return tokens;
    }

    @Override
    protected void deserialize(List<String> tokens) {
        this.rate = Double.parseDouble(tokens.get(0));
        this.deviceOccupancy = new Double(Double.parseDouble(tokens.get(1)));
    }

    @Override
    public int compareTo(HadoopCLRecording<Double> other) {
        double me = this.deviceOccupancy.doubleValue();
        double them = other.deviceOccupancy.doubleValue();
        if(me < them) {
            return -1;
        } else if (me > them) {
            return 1;
        } else {
            return 0;
        }
    }
}
