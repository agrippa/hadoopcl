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
    public float[] globalsFVal;
    public int[] globalIndices;
    public int[] globalStartingIndexPerBucket;
    public int[] globalBucketOffsets;
    public int nGlobals;
    public int globalBucketSize;

    private boolean initialized = false;

    public GlobalsWrapper() {
    }

    public void init(Configuration conf, String type) {
        if (initialized) return;

        initialized = true;
        this.globalBucketSize = conf.getInt("opencl.global.bucketsize", 50);
        try {
            final FSDataInputStream input;
            if (type == null) {
                input = FileSystem.get(conf).open(
                    new Path(conf.get("opencl.properties.globalsfile")));
            } else {
                if (type.equals("mapper")) {
                    input = FileSystem.get(conf).open(
                        new Path(conf.get("opencl.properties.globalsfile.MAPPER")));
                } else if (type.equals("combiner")) {
                    input = FileSystem.get(conf).open(
                        new Path(conf.get("opencl.properties.globalsfile.COMBINER")));
                } else if (type.equals("reducer")) {
                    input = FileSystem.get(conf).open(
                        new Path(conf.get("opencl.properties.globalsfile.REDUCER")));
                } else {
                    throw new RuntimeException("Invalid type \"" + type + "\"");
                }
            }
            final int[] metadata = ReadArrayUtils.readIntArray(input, 3);
            final int countGlobals = metadata[0];
            final int totalGlobals = metadata[1];
            final int totalGlobalBuckets = metadata[2];

            final int totalLength;
            if (type == null) {
                totalLength = (4 * countGlobals) + (4 * totalGlobals) +
                    (8 * totalGlobals) + (4 * totalGlobalBuckets) + (4 * (countGlobals + 1));
            } else {
                totalLength = (4 * countGlobals) + (4 * totalGlobals) +
                    (4 * totalGlobals) + (4 * totalGlobalBuckets) + (4 * (countGlobals + 1));
            }
            this.nGlobals = countGlobals;

            final byte[] data = ReadArrayUtils.readBytesStatic(input, totalLength);
            input.close();
            this.globalIndices = new int[countGlobals];
            this.globalsInd = new int[totalGlobals];
            if (type == null) {
                this.globalsVal = new double[totalGlobals];
            } else {
                this.globalsFVal = new float[totalGlobals];
            }
            this.globalStartingIndexPerBucket = new int[totalGlobalBuckets];
            this.globalBucketOffsets = new int[countGlobals + 1];

            int currentOffset = 0;
            final ByteBuffer bb = ByteBuffer.wrap(data);

            bb.asIntBuffer().get(this.globalIndices);
            currentOffset += this.globalIndices.length * 4;
            bb.position(currentOffset);

            bb.asIntBuffer().get(this.globalsInd);
            currentOffset += this.globalsInd.length * 4;
            bb.position(currentOffset);

            if (type == null) {
                bb.asDoubleBuffer().get(this.globalsVal);
                currentOffset += this.globalsVal.length * 8;
                bb.position(currentOffset);
            } else {
                bb.asFloatBuffer().get(this.globalsFVal);
                currentOffset += this.globalsFVal.length * 4;
                bb.position(currentOffset);
            }

            bb.asIntBuffer().get(this.globalStartingIndexPerBucket);
            currentOffset += this.globalStartingIndexPerBucket.length * 4;
            bb.position(currentOffset);

            bb.asIntBuffer().get(this.globalBucketOffsets);
        
        } catch(IOException io) {
            throw new RuntimeException(io);
        }
    }
}
