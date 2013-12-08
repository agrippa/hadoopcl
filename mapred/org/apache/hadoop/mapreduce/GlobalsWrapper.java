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
    public float[] globalsFval;
    public int[] globalIndices;
    public int nGlobals;

    public int[] globalsMapInd;
    public double[] globalsMapVal;
    public float[] globalsMapFval;
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
            int[] metadata = readIntArray(input, 2);
            int countGlobals = metadata[0];
            int totalGlobals = metadata[1];

            this.nGlobals = countGlobals;
            this.globalIndices = readIntArray(input, countGlobals);
            this.globalsInd = readIntArray(input, totalGlobals);
            this.globalsVal = readDoubleArray(input, totalGlobals);
            this.globalsFval = readFloatArray(input, totalGlobals);
            this.globalsMapInd = readIntArray(input, totalGlobals);
            this.globalsMapVal = readDoubleArray(input, totalGlobals);
            this.globalsMapFval = readFloatArray(input, totalGlobals);
            this.globalsMap = readIntArray(input, this.nGlobalBuckets * countGlobals);
            input.close();

        } catch(IOException io) {
            throw new RuntimeException(io);
        }
    }


    private int[] readIntArray(FSDataInputStream input, final int len) throws IOException {
      final int totalBytes = len * 4;
      ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);
      int nRead = 0;
      while (nRead < totalBytes) {
        int current = input.read(byteBuffer.array(), nRead, totalBytes - nRead);
        if (current < 0) {
          throw new RuntimeException("Error reading stream during readIntArray");
        }
        nRead += current;
      }
      int[] result = new int[len];
      byteBuffer.asIntBuffer().get(result);
      return result;
    }

    private float[] readFloatArray(FSDataInputStream input, final int len) throws IOException {
      final int totalBytes = len * 4;
      ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);
      int nRead = 0;
      while (nRead < totalBytes) {
        int current = input.read(byteBuffer.array(), nRead, totalBytes - nRead);
        if (current < 0) {
          throw new RuntimeException("Error reading stream during readFloatArray");
        }
        nRead += current;
      }
      float[] result = new float[len];
      byteBuffer.asFloatBuffer().get(result);
      return result;
    }

    private double[] readDoubleArray(FSDataInputStream input, final int len) throws IOException {
      final int totalBytes = len * 8;
      ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);
      int nRead = 0;
      while (nRead < totalBytes) {
        int current = input.read(byteBuffer.array(), nRead, totalBytes - nRead);
        if (current < 0) {
          throw new RuntimeException("Error reading stream during readDoubleArray");
        }
        nRead += current;
      }
      double[] result = new double[len];
      byteBuffer.asDoubleBuffer().get(result);
      return result;
    }

}
