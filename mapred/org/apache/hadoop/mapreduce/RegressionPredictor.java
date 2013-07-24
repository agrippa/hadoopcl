package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.ArrayList;

import java.io.InputStreamReader;
import java.io.BufferedReader;

public class RegressionPredictor<RecordingType> implements HadoopCLPredictor<RecordingType> {
    private DeviceFunction func;
    private Polynomial<RecordingType> poly;
    private boolean modifiable;

    public RegressionPredictor(AHadoopCLTaskCharacterization<?, RecordingType> parent) {
        this.poly = parent.getPolynomialObject(parent.getOrder(), parent.getNVariables());
        this.func = null;
        this.modifiable = true;
    }

    @Override
    public double predict(RecordingType occupancy) {
        double accum = 0.0;
        if(this.func != null) {
            double[] extended = poly.getExtendedVersion(occupancy);
            for(int i = 0; i < extended.length; i++) {
                accum = accum + (this.func.B()[i] * extended[i]);
            }
        }
        return accum;
    }

    @Override
    public void characterizationInitialization(List<HadoopCLRecording<RecordingType>> records) {
        this.recharacterize(null, records);
    }

    @Override
    public void recharacterize(HadoopCLRecording<RecordingType> newRecord, 
            List<HadoopCLRecording<RecordingType>> recordings) {
        if(!this.modifiable) {
            throw new RuntimeException("Recharacterizing a non-modifiable predictor (regression)");
        }

        StringBuffer args = new StringBuffer();
        int nsamples = recordings.size();
        args.append(nsamples); args.append(" ");

        RecordingPair<RecordingType> outliers = getOutliers(recordings);
        double[] guess = poly.plotLineBetween(outliers.P1(), outliers.P2());

        args.append(guess.length); args.append(" ");
        for(double d : guess) {
            args.append(d); args.append(" ");
        }

        synchronized(recordings) {
            for(HadoopCLRecording<RecordingType> r : recordings) {
                double[] extended = poly.getExtendedVersion(r.getOccupancy());
                for(double d : extended) {
                    args.append(d); args.append(" ");
                }
                args.append(r.getRate()); args.append(" ");
            }
        }

        DeviceFunction devFunc = getDeviceFunction("python "+System.getenv("HADOOP_APP_DIR")+"/predict.py "+args);
        if(devFunc != null) {
            this.func = devFunc;
        }
    }

    private RecordingPair<RecordingType> getOutliers(List<HadoopCLRecording<RecordingType>> recordings) {
        HadoopCLRecording<RecordingType> innerMost = null;
        HadoopCLRecording<RecordingType> outerMost = null;
        double minDist = Double.MAX_VALUE;
        double maxDist = -1.0;

        HadoopCLRecording<RecordingType> origin = recordings.get(0).getOrigin();

        for(HadoopCLRecording<RecordingType> r : recordings) {
            double dist = r.distance(origin);
            if(dist < minDist) {
                innerMost = r;
                minDist = dist;
            }
            if(dist > maxDist) {
                outerMost = r;
                maxDist = dist;
            }
        }

        return new RecordingPair<RecordingType>(innerMost, outerMost);
    }

    protected DeviceFunction getDeviceFunction(String command) {
        try {
            List<String> output = new ArrayList<String>();
            runProcess(command, output);
            
            if(output.size() > 0) {
                String bLine = output.get(0);
                String errLine = output.get(1);
                String[] tokens = bLine.split("\\s+");
                double[] B = new double[tokens.length];
                for(int i = 0; i < tokens.length; i++) {
                    B[i] = Double.parseDouble(tokens[i]);
                }
                return new DeviceFunction(B, Double.parseDouble(errLine));
            } else {
                System.out.println("DIAGNOSTICS: Failed to get device function");
            }
        } catch(NumberFormatException nfe) {
            return null;
        }

        return null;
    }

    public List<String> serializeForOutput() {
        List<String> l = new ArrayList<String>(this.func.B().length);
        for(double b : func.B()) {
            l.add(Double.toString(b));
        }
        return l;
    }

    public void initializeFromTokens(List<String> tokens) {
        double[] B = new double[tokens.size()];
        for(int i = 0; i < B.length; i++) {
            B[i] = Double.parseDouble(tokens.get(i));
        }
        this.func = new DeviceFunction(B, 0.0);
        this.modifiable = false;
    }

    private void runProcess(String command, List<String> output) {
        try {
            Process p = Runtime.getRuntime().exec(command);

            BufferedReader out = new BufferedReader(new InputStreamReader(p.getInputStream()) );
            BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()) );

            int exitCode = p.waitFor();

            if(exitCode == 0) {
                String line;
                while((line = out.readLine()) != null) {
                    output.add(line);
                }
            } else {
                StringBuffer buffer = new StringBuffer();
                String line;
                while((line = err.readLine()) != null) {
                    buffer.append(line+" | ");
                }
                //System.out.println("DIAGNOSTICS: Error running process \""+command+"\": \""+buffer.toString()+"\"");
            }
            out.close();
            err.close();

        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class RecordingPair<RecordingType> {
        private final HadoopCLRecording<RecordingType> p1;
        private final HadoopCLRecording<RecordingType> p2;

        public RecordingPair(HadoopCLRecording<RecordingType> setP1, 
                HadoopCLRecording<RecordingType> setP2) {
            this.p1 = setP1;
            this.p2 = setP2;
        }

        public HadoopCLRecording<RecordingType> P1() { return this.p1; }
        public HadoopCLRecording<RecordingType> P2() { return this.p2; }
    }
}
