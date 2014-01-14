package org.apache.hadoop.mapreduce;

import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.List;
import java.util.EnumSet;
import com.amd.aparapi.Range;
import com.amd.aparapi.internal.opencl.OpenCLPlatform;
import com.amd.aparapi.device.OpenCLDevice;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.internal.util.OpenCLUtil;
import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.io.ReadArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SparseVectorWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import java.nio.IntBuffer;
import java.nio.FloatBuffer;
import java.nio.DoubleBuffer;
import java.nio.ByteBuffer;

public class GlobalsWrapper {
    public int[] globalsInd;
    public double[] globalsVal;
    public int[] globalIndices;
    public int nGlobals;

    public int[] globalsMapInd;
    public double[] globalsMapVal;
    public int[] globalsMap;

    public int nGlobalBuckets;

    private boolean initialized = false;

    public GlobalsWrapper() {
    }

    public void init(Configuration conf) {
        if (initialized) return;

        initialized = true;
        this.nGlobalBuckets = conf.getInt("opencl.global.buckets", 4096);
        try {

            FSDataInputStream input = FileSystem.get(conf).open(
                new Path(conf.get("opencl.properties.globalsfile")));
            int[] metadata = ReadArrayUtils.readIntArray(input, 2);
            int countGlobals = metadata[0];
            int totalGlobals = metadata[1];

            this.nGlobals = countGlobals;
            this.globalIndices = ReadArrayUtils.readIntArray(input,
                    countGlobals);
            this.globalsInd = ReadArrayUtils.readIntArray(input, totalGlobals);
            this.globalsVal = ReadArrayUtils.readDoubleArray(input,
                    totalGlobals);
            this.globalsMapInd = ReadArrayUtils.readIntArray(input,
                    totalGlobals);
            this.globalsMapVal = ReadArrayUtils.readDoubleArray(input,
                    totalGlobals);
            this.globalsMap = ReadArrayUtils.readIntArray(input,
                    this.nGlobalBuckets * countGlobals);
            input.close();

        } catch(IOException io) {
            throw new RuntimeException(io);
        }
    }
}
