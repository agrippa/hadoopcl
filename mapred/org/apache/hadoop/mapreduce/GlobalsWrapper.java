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
    public int[] globalStartingIndexPerBucket;
    public int nGlobals;

    // public int[] globalsMapInd;
    // public double[] globalsMapVal;
    // public int[] globalsMap;

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

            final int totalLength = (4 * countGlobals) + (4 * totalGlobals) +
                (8 * totalGlobals) + (4 * countGlobals * nGlobalBuckets); /* + (4 * totalGlobals) + (8 * totalGlobals) +
                (4 * countGlobals * nGlobalBuckets); */

            this.nGlobals = countGlobals;

            final byte[] data = ReadArrayUtils.readBytesStatic(input, totalLength);
            input.close();
            this.globalIndices = new int[countGlobals];
            this.globalsInd = new int[totalGlobals];
            this.globalsVal = new double[totalGlobals];
            this.globalStartingIndexPerBucket = new int[nGlobalBuckets * countGlobals];
            // this.globalsMapInd = new int[totalGlobals];
            // this.globalsMapVal = new double[totalGlobals];
            // this.globalsMap = new int[nGlobalBuckets * countGlobals];

            int currentOffset = 0;
            final ByteBuffer bb = ByteBuffer.wrap(data);

            bb.asIntBuffer().get(this.globalIndices);
            currentOffset += this.globalIndices.length * 4;
            bb.position(currentOffset);

            bb.asIntBuffer().get(this.globalsInd);
            currentOffset += this.globalsInd.length * 4;
            bb.position(currentOffset);

            bb.asDoubleBuffer().get(this.globalsVal);
            currentOffset += this.globalsVal.length * 8;
            bb.position(currentOffset);

            bb.asIntBuffer().get(this.globalStartingIndexPerBucket);

            // bb.asIntBuffer().get(this.globalsMapInd);
            // currentOffset += this.globalsMapInd.length * 4;
            // bb.position(currentOffset);

            // bb.asDoubleBuffer().get(this.globalsMapVal);
            // currentOffset += this.globalsMapVal.length * 8;
            // bb.position(currentOffset);

            // bb.asIntBuffer().get(this.globalsMap);

        } catch(IOException io) {
            throw new RuntimeException(io);
        }
    }
}
