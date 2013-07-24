package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Queue;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;

public class HadoopCLTaskCharacterization extends AHadoopCLTaskCharacterization<Integer, double[]> {

    public HadoopCLTaskCharacterization(String taskName, int setTotalDevices, boolean isMapper) {
        super(setTotalDevices, taskName, isMapper);
        for(int i = 0; i < setTotalDevices; i++) {
            this.predictors.put(i, new RegressionPredictor(this));
            this.deviceToLaunches.put(i, new ArrayList<HadoopCLRecording<double[]>>());
        }
    }

    @Override
    protected int getNVariables() { return this.totalNumDevices; }
    @Override
    protected int getOrder() { return 2; }
    @Override
    protected RecordingDensity getSparsestDevice(int[] occupancy, int radius, HadoopCLDeviceChecker checker) throws IOException {
        return null;
    }
    protected Polynomial<double[]> getPolynomialObject(int order, int nvars) {
        return new ArrayPolynomial(order, nvars);
    }

    public int predictBestDevice(int[] occupancy, HadoopCLDeviceChecker checker) 
            throws IOException {
        if(!this.hasData()) {
            return -1;
        }

        double[] copy = new double[occupancy.length];
        for(int i = 0; i < copy.length; i++) copy[i] = occupancy[i];

        int bestDevice = -1;
        double bestRate = -1.0;
        for(int i = 0; i < this.totalNumDevices; i++) {
            if(checker.validDeviceForTask(i)) {
                double prediction = predict(i, copy);
                if(bestDevice == -1 || prediction > bestRate) {
                    bestRate = prediction;
                    bestDevice = i;
                }
            }
        }
        return bestDevice;
    }
}
