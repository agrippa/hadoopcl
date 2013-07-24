package org.apache.hadoop.mapreduce;

public class ArrayPolynomial extends Polynomial<double[]> {
    public ArrayPolynomial(int order, int nvars) {
        super(order, nvars);
    }
    public double[] getExtendedVersion(double[] sample) {
        return super.getExtendedVersion(sample);
    }

    public double[] plotLineBetween(HadoopCLRecording<double[]> p1,
            HadoopCLRecording<double[]> p2) {
        if(p1 == p2) {
            double[] dummy = new double[this.nTerms()];
            for(int i = 0; i < dummy.length; i++) {
                dummy[i] = 0.0;
            }
            return dummy;
        }

        HadoopCLRecording<double[]> origin = p1.getOrigin();
        double p1DistFromOrigin = p1.distance(origin);
        double p2DistFromOrigin = p2.distance(origin);

        HadoopCLRecording<double[]> lesser = (p1DistFromOrigin < p2DistFromOrigin ? p1 : p2);
        HadoopCLRecording<double[]> greater = (lesser == p1 ? p2 : p1);

        double[] slopes = new double[lesser.getOccupancy().length];
        for(int i = 0; i < slopes.length; i++) {
            slopes[i] = (greater.getRate()-lesser.getRate()) / 
                (greater.getOccupancy()[i] - lesser.getOccupancy()[i]);
        }
        double b = lesser.getRate() - (slopes[0] * lesser.getOccupancy()[0]);

        double[] equation = new double[this.nTerms()];
        int termCount = 0;
        for(Weights w : this) {
            int isLinear = w.isLinear();
            if(w.allZeros()) {
                equation[termCount] = b;
            } else if(isLinear >= 0) {
                equation[isLinear] = slopes[isLinear];
            }
            termCount++;
        }

        return equation;
    }
}
