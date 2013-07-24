package org.apache.hadoop.mapreduce;

public class DoublePolynomial extends Polynomial<Double> {
    public DoublePolynomial(int order, int nvars) {
        super(order, nvars);
    }
    public double[] getExtendedVersion(Double sample) {
        return this.getExtendedVersion(new double[] { sample.doubleValue() });
    }

    public double[] plotLineBetween(HadoopCLRecording<Double> p1,
            HadoopCLRecording<Double> p2) {
        if(p1 == p2) {
            return new double[] { 0.0, 0.0 };
        }

        HadoopCLRecording<Double> greater = (p2.getOccupancy().doubleValue() > p1.getOccupancy().doubleValue() ? p2 : p1);
        HadoopCLRecording<Double> lesser = (greater == p1 ? p2 : p1);

        //System.out.println("DIAGNOSTICS: Plotting line from ("+lesser.getOccupancy().doubleValue()+", "+
        //        lesser.getRate()+") to ("+greater.getOccupancy().doubleValue()+", "+greater.getRate()+")");
        double slope = (greater.getRate()-lesser.getRate()) / 
            (greater.getOccupancy().doubleValue() - lesser.getOccupancy().doubleValue());
        double b = lesser.getRate() - (slope * lesser.getOccupancy().doubleValue());
        //System.out.println("DIAGNOSTICS: slope="+slope+", b="+b);

        double[] equation = new double[2];
        int termCount = 0;
        for(Weights w : this) {
            //System.out.println("DIAGNOSTICS: Weight "+w.toString()+" "+w.allZeros()+" "+w.isLinear());
            if(w.allZeros()) {
                equation[termCount] = b;
            } else if(w.isLinear() >= 0) {
                equation[termCount] = slope;
            }
            termCount++;
        }
        return equation;
    }
}
