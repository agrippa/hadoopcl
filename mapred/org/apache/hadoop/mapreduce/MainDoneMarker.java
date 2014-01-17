package org.apache.hadoop.mapreduce;

import java.io.IOException;

public class MainDoneMarker extends HadoopCLInputBuffer {
    public static final MainDoneMarker SINGLETON = new MainDoneMarker();

    public void reset() { }
    public void addTypedKey(Object obj) { }
    public void addTypedValue(Object obj) { }
    public boolean hasWork() { return false; }
    public void addKeyAndValue(TaskInputOutputContext ctx)
        throws IOException, InterruptedException { }
    public boolean isFull(TaskInputOutputContext ctx) { return true; }
    public void init(int pairsPerInput, HadoopOpenCLContext ctx) { }
    public boolean completedAll() { return true; }
}
