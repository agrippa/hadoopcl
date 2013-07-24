package org.apache.hadoop.mapreduce;

import java.util.Set;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Queue;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;

import com.amd.aparapi.device.OpenCLDevice;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.device.Device.TYPE;

public class HadoopCLTaskCharacterization3 extends AHadoopCLTaskCharacterization<Device.TYPE, Double> {
    private final List<Device.TYPE> deviceTypes;

    public HadoopCLTaskCharacterization3(String taskName, int setTotalNumDevices, List<Device.TYPE> deviceTypes, boolean isMapper) {
        super(setTotalNumDevices, taskName, isMapper);
        this.deviceTypes = deviceTypes;
        for(Device.TYPE t : this.deviceTypes) {
            if(!this.predictors.containsKey(t)) {
                this.predictors.put(t, new RegressionPredictor(this));
                this.deviceToLaunches.put(t, new ArrayList<HadoopCLRecording<Double>>());
            }
        }
    }

    @Override
    protected int getNVariables() { return 1; }
    @Override
    protected int getOrder() { return 2; }
    @Override
    protected RecordingDensity getSparsestDevice(int[] occupancy, int radius, HadoopCLDeviceChecker checker) throws IOException {
        return null;
    }
    protected Polynomial<Double> getPolynomialObject(int order, int nvars) {
        return new DoublePolynomial(order, nvars);
    }

    public int predictBestDevice(int[] occupancy, HadoopCLDeviceChecker checker)
            throws IOException {
        if(!this.hasData()) {
            return -1;
        }

        int bestDevice = -1;
        double bestRate = -1.0;
        for(int i = 0; i < this.totalNumDevices; i++) {
            Device.TYPE type = this.deviceTypes.get(i);
            if(checker.validDeviceForTask(type)) {
                double prediction = predict(type, (double)occupancy[i]);
                if(bestDevice == -1 || prediction > bestRate) {
                    bestRate = prediction;
                    bestDevice = i;
                }
            }
        }
        return bestDevice;
    }
}
