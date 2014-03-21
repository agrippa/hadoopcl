package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus;

import java.io.IOException;
import java.util.jar.JarFile;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.net.URL;
import java.net.URLClassLoader;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AHadoopCLTaskCharacterization<MappingType, RecordingType> {

    private AtomicInteger devicePointer;
    protected final int totalNumDevices;
    protected final String taskName;
    private final boolean isMapperTask;
    private final AtomicReference<HadoopCLKernel> kernelObject;

    protected final ConcurrentHashMap<MappingType, HadoopCLPredictor> predictors;
    protected final ConcurrentHashMap<MappingType, List<HadoopCLRecording<RecordingType>>> deviceToRecordings;
    protected final ConcurrentHashMap<MappingType, List<HadoopCLRecording<RecordingType>>> deviceToLaunches;
    private final Polynomial<RecordingType> poly;
    private AtomicBoolean canPredict;

    private boolean initializing = true;
    final private HashMap<MappingType, Integer> nLaunchesLoaded = new HashMap<MappingType, Integer>();
    final private HashMap<MappingType, Integer> nRecordingsLoaded = new HashMap<MappingType, Integer>();

	public AHadoopCLTaskCharacterization(int setTotalNumDevices, 
            String taskName, boolean isMapper) {
        this.devicePointer = new AtomicInteger(0);
        this.totalNumDevices = setTotalNumDevices;
        this.taskName = taskName;
        this.predictors = new ConcurrentHashMap<MappingType, HadoopCLPredictor>();
        this.deviceToRecordings = new ConcurrentHashMap<MappingType,
            List<HadoopCLRecording<RecordingType>>>();
        this.deviceToLaunches = new ConcurrentHashMap<MappingType,
            List<HadoopCLRecording<RecordingType>>>();
        this.poly = getPolynomialObject(this.getOrder(), this.getNVariables());
        this.canPredict = new AtomicBoolean(false);
        this.isMapperTask = isMapper;
        this.kernelObject = new AtomicReference<HadoopCLKernel>(null);
	}

    public abstract int predictBestDevice(int[] occupancy, HadoopCLDeviceChecker checker) throws IOException;
    protected abstract int getNVariables();
    protected abstract int getOrder();
    protected abstract Polynomial<RecordingType> getPolynomialObject(int order, int nvars);
    protected abstract RecordingDensity getSparsestDevice(int[] occupancy,
            int radius, HadoopCLDeviceChecker checker) throws IOException;

    public void finishInitializing() {
        this.initializing = false;
    }

    protected boolean failingDevice(MappingType m) {
        final int launches, recordings;
        if (!nLaunchesLoaded.containsKey(m)) {
            launches = 0;
        } else {
            launches = nLaunchesLoaded.get(m);
        }
        if (!nRecordingsLoaded.containsKey(m)) {
            recordings = 0;
        } else {
            recordings = nRecordingsLoaded.get(m);
        }
        return ((double)recordings / (double)launches) < 0.5;
    }

    public HadoopCLKernel getKernelObject(Task task, JobConf conf) throws IOException {
        if(this.kernelObject.get() != null) {
            return this.kernelObject.get();
        }

        HadoopCLKernel kernel = HadoopCLScheduler.getKernelForTask(task, conf);
        this.kernelObject.set(kernel); // doesn't matter who succeeds in setting this
        return kernel;
    }

    public void unsafeAddLaunch(MappingType m, HadoopCLRecording<RecordingType> record) {
        if (initializing) {
            if (this.nLaunchesLoaded.containsKey(m)) {
                this.nLaunchesLoaded.put(m, new Integer(
                            this.nLaunchesLoaded.get(m).intValue() + 1));
            } else {
                this.nLaunchesLoaded.put(m, new Integer(1));
            }
        }
        this.deviceToLaunches.get(m).add(record);
    }

    public void addLaunch(MappingType m, HadoopCLRecording<RecordingType> record) {
        List<HadoopCLRecording<RecordingType>> newList = this.deviceToLaunches.get(m);

        synchronized(newList) {
            newList.add(record);
        }
    }

    protected int countLaunchesNear(MappingType device, int[] occupancy, int radius) {
        int count = 0;
        List<HadoopCLRecording<RecordingType>> recordings = this.deviceToLaunches.get(device);
        //System.out.println("DIAGNOSTICS: Launches null ? "+(recordings == null));
        if(recordings != null) {
            synchronized(recordings) {
                //System.out.println("DIAGNOSTICS: # launches = "+recordings.size());
                for(HadoopCLRecording<RecordingType> r : recordings) {
                    if(r.distance(occupancy) < radius) {
                        count = count + 1;
                    }
                }
            }
        }
        return count;
    }

    public HadoopCLPredictor getPredictor(MappingType m) {
        return this.predictors.get(m);
    }

    public void initPredictor(MappingType m, List<String> tokens) {
        this.predictors.get(m).initializeFromTokens(tokens);
        //this.canPredict = true;
        this.canPredict.set(true);
    }

    protected double predict(MappingType m, RecordingType occupancy) {
        HadoopCLPredictor p = this.predictors.get(m);
        if(p != null) {
            double prediction = p.predict(occupancy);
            return prediction; 
        } else {
            return 0.0;
        }
    }

    public void characterizationInitialization() {
        for(MappingType m : this.deviceToRecordings.keySet()) {
            List<HadoopCLRecording<RecordingType>> recordings = this.deviceToRecordings.get(m);
            this.predictors.get(m).characterizationInitialization(recordings);
        }
        //this.canPredict = true;
        this.canPredict.set(true);
    }

    private void recharacterize(MappingType m, HadoopCLRecording<RecordingType> cause) {
        List<HadoopCLRecording<RecordingType>> readings = this.deviceToRecordings.get(m);
        this.predictors.get(m).recharacterize(cause, readings);
        //this.canPredict = true;
        this.canPredict.set(true);
    }

    protected void recordingHelper(MappingType m, HadoopCLRecording<RecordingType> record, 
            ConcurrentHashMap<MappingType, List<HadoopCLRecording<RecordingType>>> storage) {
        List<HadoopCLRecording<RecordingType>> newList = new ArrayList<HadoopCLRecording<RecordingType>>();
        newList = storage.putIfAbsent(m, newList);
        if(newList == null) newList = storage.get(m);

        synchronized(newList) {
            newList.add(record);
        }
    }

    public void unsafeAddRecording(MappingType m, HadoopCLRecording<RecordingType> record) {
        if (initializing) {
            if (this.nRecordingsLoaded.containsKey(m)) {
                this.nRecordingsLoaded.put(m, new Integer(
                            this.nRecordingsLoaded.get(m).intValue() + 1));
            } else {
                this.nRecordingsLoaded.put(m, new Integer(1));
            }
        }
        if(!this.deviceToRecordings.containsKey(m)) {
            this.deviceToRecordings.put(m, new ArrayList<HadoopCLRecording<RecordingType>>());
        }
        List<HadoopCLRecording<RecordingType>> newList = this.deviceToRecordings.get(m);
        newList.add(record);

        //this.recharacterize(m, record);
    }

    public void addRecording(MappingType m, HadoopCLRecording<RecordingType> record) {
        recordingHelper(m, record, this.deviceToRecordings);

        this.recharacterize(m, record);
    }

    public boolean hasData() {
        //return this.canPredict;
        return this.canPredict.get();
    }
	
	public int getNextMissingDevice(int[] occupancy, HadoopCLDeviceChecker checker)
            throws IOException {
        RecordingDensity density;
        if(this.isMapperTask) {
            density = this.getSparsestDevice(occupancy, 4, checker);
        } else {
            density = this.getSparsestDevice(occupancy, 2, checker);
        }
        if(!this.hasData() || density.density() == 0.0) {
            // StringBuffer sb = new StringBuffer();
            // sb.append("DIAGNOSTICS: Recording density of ");
            // sb.append(density.density());
            // sb.append(" for device ");
            // sb.append(density.device());
            // sb.append(" at occupancy [ ");
            // for(int i : occupancy) {
            //     sb.append(i);
            //     sb.append(" ");
            // }
            // sb.append("]");
            // System.out.println(sb.toString());
            return density.device();
        }
        return -1;
    }
}
