package org.apache.hadoop.mapreduce;

import java.io.IOException;

public class MainDoneMarker extends HadoopCLInputBuffer {
    public MainDoneMarker(HadoopOpenCLContext clContext) {
        super(clContext, -1);
    }

    public void reset() { }
    public void addTypedKey(Object obj) { }
    public void addTypedValue(Object obj) { }
    public boolean hasWork() { return false; }
    public void addKeyAndValue(TaskInputOutputContext ctx)
        throws IOException, InterruptedException { }
    public boolean isFull(TaskInputOutputContext ctx) { return true; }
    public void init(int pairsPerInput, HadoopOpenCLContext ctx) { }
    public boolean completedAll() { return true; }
    public int bulkFill(HadoopCLDataInput stream) { return 0; }
    public void printContents() { }
    public void clearNWrites() { }
    public int getNInputs() { return 0; }
}
