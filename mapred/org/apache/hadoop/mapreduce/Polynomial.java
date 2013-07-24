package org.apache.hadoop.mapreduce;

import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;

public abstract class Polynomial<RecordingType> implements Iterable<Weights> {
    private final int order;
    private final int nvars;
    private final Set<Weights> possibles;

    private void recurse(int[] current, int index) {
        for(int i =index; i < current.length; i++) {
            current[i] = 0;
        }

        int sum = 0;
        for(int i = 0; i < index; i++) {
            sum = sum + current[i];
        }

        while((sum+current[index]) <= order) {
            possibles.add(new Weights(current));

            if(index < current.length-1) {
                recurse(current, index+1);
                for(int i = index+1; i < current.length; i++) {
                    current[i] = 0;
                }
            }
            current[index] = current[index] + 1;
        }
    }

    public Polynomial(int order, int nvars) {
        this.order = order;
        this.nvars = nvars;

        possibles = new TreeSet<Weights>();
        int[] current = new int[nvars];
        recurse(current, 0);
    }

    public Iterator<Weights> iterator() {
        return possibles.iterator();
    }

    public int nTerms() {
        return possibles.size();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        for(Weights w : possibles) {
            if(!first) sb.append(" ");
            sb.append("[ ");
            for(int i : w.weights()) {
                sb.append(i);
                sb.append(" ");
            }
            sb.append("]");
            first = false;
        }
        return sb.toString();
    }

    public abstract double[] getExtendedVersion(RecordingType sample);

    protected double[] getExtendedVersion(double[] sample) {
        double[] extended = new double[this.nTerms()];
        int termCount = 0;
        for(Weights w : possibles) {
            extended[termCount] = evaluateTerm(w, sample);
            termCount++;
        }
        return extended;
    }

    private double evaluateTerm(Weights term, double[] sample) {
        double val = 1.0;

        for(int i = 0; i < term.weights().length; i++) {
            int t = term.weights()[i];
            if(t > 0) {
                val = val * Math.pow(sample[i], t);
            }
        }
        return val;
    }

    public abstract double[] plotLineBetween(HadoopCLRecording<RecordingType> p1,
            HadoopCLRecording<RecordingType> p2);
}
