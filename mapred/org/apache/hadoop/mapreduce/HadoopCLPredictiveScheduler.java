package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.JobConf;
import java.io.IOException;
import java.io.BufferedWriter;
import com.amd.aparapi.device.Device;
import java.util.HashMap;
import java.util.jar.JarFile;
import java.util.Enumeration;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.JarEntry;
import java.util.List;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.ConcurrentHashMap;

public abstract class HadoopCLPredictiveScheduler<MappingType, RecordingType> extends HadoopCLScheduler {
    protected BufferedWriter recordingsWriter = null;
    protected BufferedWriter launchesWriter = null;
    protected final ConcurrentHashMap<String, AHadoopCLTaskCharacterization<MappingType, RecordingType>> taskPerfProfile;

    public HadoopCLPredictiveScheduler(JobConf conf) {
        super(conf);
        this.taskPerfProfile = new ConcurrentHashMap<String,
            AHadoopCLTaskCharacterization<MappingType, RecordingType>>();
    }

	public abstract int shouldSwitchPlatform(Task task, JobConf conf, 
            long expectedRemaining, long expectedTotalInputs) throws IOException;
    public abstract AHadoopCLTaskCharacterization<MappingType, RecordingType> getCharacterizationObject(String taskName,
            List<Device.TYPE> deviceTypes, boolean isMapper);
    public abstract HadoopCLRecording<RecordingType> getRecordingObject(int device, double rate, double[] setOccupancy);
    public abstract MappingType getMappingObject(int device);
    public abstract boolean recordLaunches();

    public void setRecordingsFile(BufferedWriter writer) {
        this.recordingsWriter = writer;
    }

    public void setLaunchesFile(BufferedWriter writer){
        this.launchesWriter = writer;
    }

    protected void loadFromDirectory(String dirname) {
        loadRecordingsFromFile(dirname+"/recordings.saved");
        loadLaunchesFromFile(dirname+"/launches.saved");
        for (String s : taskPerfProfile.keySet()) {
            taskPerfProfile.get(s).finishInitializing();
        }
    }

    private void loadLaunchesFromFile(String filename) {
        long start = System.currentTimeMillis();
        int nlaunches = 0;
        try {
            File file = new File(filename);
            if (file.exists() && file.isFile()) {
                BufferedReader reader = new BufferedReader(new FileReader(filename));
                String line = "";
                while((line = reader.readLine()) != null) {
                    String[] tokens = line.split(" ");
                    String taskName = tokens[0];
                    String deviceStr = tokens[1];
                    String mapperStr = tokens[2];
                    int device = Integer.parseInt(deviceStr);
                    boolean isMapper = Boolean.parseBoolean(mapperStr);

                    int occIndex = 4;
                    double[] tmpOccupancy = new double[deviceOccupancy.length];
                    while(occIndex < tokens.length && !tokens[occIndex].equals("]")) {
                        tmpOccupancy[occIndex - 4] = Double.parseDouble(tokens[occIndex]);
                        occIndex++;
                    }

                    checkTaskType(taskName, isMapper, true);
                    taskPerfProfile.get(taskName).unsafeAddLaunch(
                        this.getMappingObject(device), 
                        this.getRecordingObject(device, 0.0, tmpOccupancy));
                    nlaunches++;
                }
                reader.close();
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        long stop = System.currentTimeMillis();
        System.out.println("DIAGNOSTICS: Loading " + nlaunches + " launches from file took "+(stop-start)+" ms");

    }

    protected void checkTaskType(String taskName, boolean isMapper,
            boolean threadSafe) {
        double[] emptyOccupancy = new double[deviceOccupancy.length];
        for (int i = 0; i < emptyOccupancy.length; i++) {
            emptyOccupancy[i] = 0;
        }
        if (threadSafe) {
            if(!taskPerfProfile.containsKey(taskName)) {
                taskPerfProfile.put(taskName, 
                    this.getCharacterizationObject(taskName,
                        this.deviceTypes, isMapper));
                for (int i = 0; i < this.deviceOccupancy.length; i++) {
                    taskPerfProfile.get(taskName).unsafeAddRecording(
                            this.getMappingObject(i),
                            this.getRecordingObject(i, 0.0, emptyOccupancy));
                }
            }
        } else {
            AHadoopCLTaskCharacterization<MappingType, RecordingType> taskProfile =
                this.getCharacterizationObject(taskName, this.deviceTypes,
                        isMapper);
            for (int i = 0; i < this.deviceOccupancy.length; i++) {
                taskProfile.unsafeAddRecording(this.getMappingObject(i),
                        this.getRecordingObject(i, 0.0, emptyOccupancy));
            }
            taskPerfProfile.putIfAbsent(taskName, taskProfile);
        }
    }

    private void loadRecordingsFromFile(String filename) {
        long start = System.currentTimeMillis();
        int nrecordings = 0;
        String line = "";
        try {
            File file = new File(filename);
            if (file.exists() && file.isFile()) {
                BufferedReader reader = new BufferedReader(new FileReader(filename));
                while((line = reader.readLine()) != null) {
                    String[] tokens = line.split(" ");
                    String taskName = tokens[0];
                    String deviceStr = tokens[1];
                    String rateStr = tokens[2];
                    String mapperStr = tokens[3];
                    int device = Integer.parseInt(deviceStr);
                    boolean isMapper = Boolean.parseBoolean(mapperStr);

                    int occIndex = 5;
                    double[] tmpOccupancy = new double[deviceOccupancy.length];
                    while(occIndex < tokens.length && !tokens[occIndex].equals("]")) {
                        tmpOccupancy[occIndex - 5] = Double.parseDouble(tokens[occIndex]);
                        occIndex++;
                    }

                    checkTaskType(taskName, isMapper, true);
                    HadoopCLRecording<RecordingType> recording =
                        this.getRecordingObject(device,
                                Double.parseDouble(rateStr), tmpOccupancy);
                    MappingType map = this.getMappingObject(device);
                    this.taskPerfProfile.get(taskName).unsafeAddRecording(map, recording);
                    nrecordings++;
                }
                reader.close();
            }
        } catch(Exception e) {
            throw new RuntimeException(line, e);
        }
        for(String key : taskPerfProfile.keySet()) {
            this.taskPerfProfile.get(key).characterizationInitialization();
        }
        long stop = System.currentTimeMillis();
        System.out.println("DIAGNOSTICS: Loading " + nrecordings + " recordings from file took "+(stop-start)+" ms");
    }

    protected void writeLaunch(String taskName, int device, int[] occupancy, boolean isMapper) {
        try {
            if(this.launchesWriter != null && recordLaunches()) {
                StringBuffer buffer = new StringBuffer();
                buffer.append(taskName);
                buffer.append(" ");
                buffer.append(device);
                buffer.append(" ");
                buffer.append(Boolean.toString(isMapper));
                buffer.append(" [");
                for(int i = 0; i < occupancy.length; i++) {
                    buffer.append(" ");
                    buffer.append(occupancy[i]);
                }
                buffer.append(" ]\n");

                this.launchesWriter.write(buffer.toString());
                this.launchesWriter.flush();
            }
        } catch(IOException io) {
            throw new RuntimeException(io);
        }
    }

    protected void writeRecording(String taskName, int device, double rate, double[] occupancy, boolean isMapper,
            HadoopCLPredictor pred) {
        try {
            if(this.recordingsWriter != null) {
                StringBuffer buffer = new StringBuffer();
                buffer.append(taskName);
                buffer.append(" ");
                buffer.append(device);
                buffer.append(" ");
                buffer.append(rate);
                buffer.append(" ");
                buffer.append(Boolean.toString(isMapper));
                buffer.append(" [");
                for(int i = 0; i < occupancy.length; i++) {
                    buffer.append(" ");
                    buffer.append(occupancy[i]);
                }
                buffer.append(" ] [");
                List<String> tokens = pred.serializeForOutput();
                for(String t : tokens) {
                    buffer.append(" ");
                    buffer.append(t);
                }
                buffer.append(" ] ");
                buffer.append("\n");

                this.recordingsWriter.write(buffer.toString());
                this.recordingsWriter.flush();
            }
        } catch(IOException io) {
            throw new RuntimeException(io);
        }
    }

    @Override
    public DeviceAssignment bestCandidateDevice(Task task, JobConf conf) 
            throws IOException {

        String taskClassName = task.getMainClassName(conf);
        if (taskClassName == null) {
            return new DeviceAssignment(deviceOccupancy.length-1, -1, false); // running in Java
        }
        AHadoopCLTaskCharacterization<MappingType, RecordingType> taskProfile = 
            getCharacterizationObject(taskClassName, this.deviceTypes, task.isMapTask());
        taskProfile = taskPerfProfile.putIfAbsent(taskClassName, taskProfile);
        if(taskProfile == null) taskProfile = taskPerfProfile.get(taskClassName);

        int[] occupancyCopy = new int[this.deviceOccupancy.length];
        double[] dOccupancyCopy = new double[this.deviceOccupancy.length];
        synchronized(this.deviceOccupancy) {
            for(int i = 0; i < occupancyCopy.length; i++) {
                occupancyCopy[i] = this.deviceOccupancy[i];
                dOccupancyCopy[i] = this.deviceOccupancy[i];
            }
        }

        DeviceAssignment result = null;

        int optimalDevice = taskProfile.getNextMissingDevice(occupancyCopy,
                new HadoopCLDeviceChecker(task, conf, taskProfile, this));
        if(optimalDevice == -1) {
            optimalDevice = taskProfile.predictBestDevice(occupancyCopy,
                    new HadoopCLDeviceChecker(task, conf, taskProfile, this));
            result = new DeviceAssignment(optimalDevice, -1, false);
        } else {
            result = new DeviceAssignment(optimalDevice, -1, true);
        }

        writeLaunch(taskClassName, optimalDevice, occupancyCopy, task.isMapTask());
        taskProfile.addLaunch(this.getMappingObject(optimalDevice), 
            this.getRecordingObject(optimalDevice, 0.0, dOccupancyCopy));

        synchronized(this.deviceOccupancy) {
            this.deviceOccupancy[optimalDevice]++;
        }

        taskToDevice.put(task.getTaskID(), new DeviceLoad(optimalDevice, -1,
              -1.0, deviceOccupancy));
        return result;
    }
}
