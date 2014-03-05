package org.apache.hadoop.mapreduce;

public class DeviceLoad {
    private final int deviceID;
    private final int deviceSlot;
    private final double load;
    private final int[] occupancy;
    public DeviceLoad(int setDevice, int deviceSlot, double setLoad,
            int[] currentOccupancy) {
        this.deviceID = setDevice;
        this.deviceSlot = deviceSlot;
        this.load = setLoad;
        this.occupancy = new int[currentOccupancy.length];
        for(int i = 0; i < this.occupancy.length; i++) {
            this.occupancy[i] = currentOccupancy[i];
        }
    }
    public int getDevice() {
        return deviceID;
    }
    public int getDeviceSlot() {
        return deviceSlot;
    }
    public double getLoad() {
        return load;
    }
    public int[] getOccupancy() {
        return this.occupancy;
    }
}

