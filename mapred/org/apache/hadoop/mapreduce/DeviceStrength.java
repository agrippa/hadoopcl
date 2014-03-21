package org.apache.hadoop.mapreduce;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.HashMap;
import com.amd.aparapi.device.Device;

public class DeviceStrength {
    private int strengthSum = 0;
    private final Random rand = new Random();
    private final HashMap<Device.TYPE, Integer> strength = new HashMap<Device.TYPE, Integer>();

    public void add(Device.TYPE type, int val) {
        strengthSum += val;
        strength.put(type, new Integer(val));
    }

    public int get(Device.TYPE type) {
        if(strength.containsKey(type)) {
            return strength.get(type);
        } else {
            return 0;
        }
    }

    public Device.TYPE randomlySelectedDeviceType(int selection) {
        selection = selection % strengthSum;
        Iterator<Map.Entry<Device.TYPE, Integer>> iter = strength.entrySet().iterator();
        int base = 0;
        Map.Entry<Device.TYPE, Integer> curr = iter.next();
        while (iter.hasNext() && base + curr.getValue().intValue() <= selection) {
            curr = iter.next();
        }
        return curr.getKey();
    }

    public void clear() {
        strength.clear();
        strengthSum = 0;
    }
}
