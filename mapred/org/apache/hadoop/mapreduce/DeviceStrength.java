package org.apache.hadoop.mapreduce;

import com.amd.aparapi.device.Device;
import java.util.HashMap;

public class DeviceStrength {
    private HashMap<Device.TYPE, Integer> strength;

    public DeviceStrength() {
        this.strength = new HashMap<Device.TYPE, Integer>();
    }

    public void add(Device.TYPE type, int val) {
        strength.put(type, new Integer(val));
    }

    public int get(Device.TYPE type) {
        if(strength.containsKey(type)) {
            return strength.get(type);
        } else {
            return 0;
        }
    }

    public void clear() {
        strength.clear();
    }
}
