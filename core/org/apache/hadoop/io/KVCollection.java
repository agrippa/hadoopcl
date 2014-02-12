package org.apache.hadoop.io;

import java.io.IOException;
import java.io.DataOutputStream;

public interface KVCollection<K, V> extends Iterable<Integer> {
    public int start();
    public int end();
    public void serializeKey(int index, DataOutputStream out) throws IOException;
    public void serializeValue(int index, DataOutputStream out) throws IOException;
    public boolean isValid(int index);
    public Writable getKeyFor(int index, Writable w);
    public Writable getValueFor(int index, Writable w);
    public int getPartitionFor(int index,
        int numReduceTasks);
}
