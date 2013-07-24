package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Set;
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

public class HadoopCLStaticTaskCharacterization extends AHadoopCLTaskCharacterization<Device.TYPE, Double> {
    private final List<Device.TYPE> deviceTypes;

    public HadoopCLStaticTaskCharacterization(String taskName, int setTotalNumDevices, List<Device.TYPE> deviceTypes, boolean isMapper) {
        super(setTotalNumDevices, taskName, isMapper);
        this.deviceTypes = deviceTypes;
        for(Device.TYPE t : this.deviceTypes) {
            if(!this.predictors.containsKey(t)) {
                if(t == Device.TYPE.GPU) {
                    this.predictors.put(t, new NearestPredictor());
                } else {
                    this.predictors.put(t, new RegressionPredictor(this));
                }
                this.deviceToLaunches.put(t, new ArrayList<HadoopCLRecording<Double>>());
            }
        }
    }

    @Override
    protected int getNVariables() { return 1; }
    @Override
    protected int getOrder() { return 1; }
    @Override
    protected RecordingDensity getSparsestDevice(int[] occupancy, int radius, HadoopCLDeviceChecker checker) throws IOException {
        return new RecordingDensity(-1, 100.0); // to avoid using this
    }

    protected Polynomial<Double> getPolynomialObject(int order, int nvars) {
        return new DoublePolynomial(order, nvars);
    }

    public int predictBestDevice(int[] occupancy, HadoopCLDeviceChecker checker) 
            throws IOException {
        if(!this.hasData()) {
            return -1;
        }

		double bestRate = -1.0;
		Device.TYPE bestDevice = Device.TYPE.UNKNOWN;
        for(int i = 0; i < occupancy.length; i++) {
            Device.TYPE type = this.deviceTypes.get(i);
            if(checker.validDeviceForTask(type)) {
                double prediction = predict(type, (double)(occupancy[i]));
                if(prediction > bestRate) {
                    bestRate = prediction;
                    bestDevice = type;
                }
            }
		}
	
        int minOcc = -1;
        int minOccDevice = -1;

        for(int i = 0; i < this.deviceTypes.size(); i++) {
            if(this.deviceTypes.get(i) == bestDevice) {
                if(minOccDevice == -1 || occupancy[i] < minOcc) {
                    minOcc = occupancy[i];
                    minOccDevice = i;
                }
            }
        }
		
		return minOccDevice;
    }
}
