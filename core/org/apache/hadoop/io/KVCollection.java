package org.apache.hadoop.io;

import java.io.IOException;
import java.io.DataOutputStream;

public interface KVCollection<K, V> {
    public int start();
    public int end();
    public void serializeKey(int index, DataOutputStream out) throws IOException;
    public void serializeValue(int index, DataOutputStream out) throws IOException;
    public boolean isValid(int index);
    public K getKeyFor(int index);
    public V getValueFor(int index);
    public int getPartitionFor(int index,
        int numReduceTasks);
}
