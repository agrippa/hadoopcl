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

public class HadoopCLTaskCharacterization4 extends AHadoopCLTaskCharacterization<Device.TYPE, Double> {
    private final List<Device.TYPE> deviceTypes;

    public HadoopCLTaskCharacterization4(String taskName, int setTotalNumDevices, List<Device.TYPE> deviceTypes, boolean isMapper) {
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
        double lowestDensity = Double.MAX_VALUE;
        List<Device.TYPE> eligible = new ArrayList<Device.TYPE>();
        int deviceIndex = 0;

        for(Device.TYPE t : this.deviceTypes) {
            if(!checker.validDeviceForTask(t)) continue;
            int area = 1;
            for(int i = 0; i < occupancy.length; i++) {
                int low = occupancy[i] - radius;
                if(low < 0) low = 0;
                int high = occupancy[i] + radius;
                area = area * (high - low);
            }

            int count = 0;
            List<HadoopCLRecording<Double>> recordings = this.deviceToRecordings.get(t);
            if(recordings != null) {
                synchronized(recordings) {
                    for(HadoopCLRecording<Double> r : recordings) {
                        double[] occ = r.fullOccupancy();
                        if(r.distance(occupancy) < radius) {
                            count++;
                        }
                    }
                }
            }
            double density;
            if(count == 0) {
                density = 0.0;
            } else {
                density = (double)count / (double)area;
            }

            if(density < lowestDensity) {
                eligible = new ArrayList<Device.TYPE>();
                eligible.add(t);
                lowestDensity = density;
            } else if(density == lowestDensity) {
                if(!eligible.contains(t)) {
                    eligible.add(t);
                }
            }

            deviceIndex = deviceIndex + 1;
        }
        //System.out.println("DIAGNOSTICS: After first loop, lowestDensity="+lowestDensity+", # eligible devices="+eligible.size());

        List<Device.TYPE> trulyEligible;
        if(lowestDensity == 0.0) {
            trulyEligible = new ArrayList<Device.TYPE>();
            deviceIndex = 0;
            for(Device.TYPE t : this.deviceTypes) {
                if(eligible.contains(t) && !trulyEligible.contains(t)) {
                    int launchesNear = countLaunchesNear(t, occupancy, radius);
                    //System.out.println("DIAGNOSTICS: For device "+deviceIndex+" got "+launchesNear+" launches nearby");
                    if(launchesNear < 3) {
                        trulyEligible.add(t);
                    }
                }
                deviceIndex++;
            }
        } else {
            trulyEligible = eligible;
        }

        int lowestDeviceIndex = -1;
        int lowestDeviceOcc = Integer.MAX_VALUE;
        deviceIndex = 0;
        for(Device.TYPE t : this.deviceTypes) {
            if(trulyEligible.contains(t) && occupancy[deviceIndex] < lowestDeviceOcc) {
                lowestDeviceIndex = deviceIndex;
                lowestDeviceOcc = occupancy[deviceIndex];
            }
            deviceIndex = deviceIndex + 1;
        }
        return new RecordingDensity(lowestDeviceIndex, lowestDensity);
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
