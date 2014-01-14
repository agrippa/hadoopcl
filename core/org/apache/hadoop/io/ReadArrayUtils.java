package org.apache.hadoop.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.nio.IntBuffer;
import java.nio.FloatBuffer;
import java.nio.DoubleBuffer;
import java.nio.ByteBuffer;

public class ReadArrayUtils {
    // public static final ReadArrayUtils util = new ReadArrayUtils();
    // public static final ReadArrayUtils forCombiners = new ReadArrayUtils();

    private ByteBuffer dumpBuffer = null;
    private int dumpLength = -1;

    // private byte[] readBuffer = null;
    // private int readLength = -1;

    private void ensureDumpCapacity(int len) {
        if (this.dumpLength < len) {
            dumpBuffer = ByteBuffer.allocate(len);
            dumpLength = len;
        }
    }

    // private void ensureReadCapacity(int len) {
    //     if (this.readLength < len) {
    //         readBuffer = new byte[len];
    //         readLength = len;
    //     }
    // }

    private static byte[] readBytes(DataInput input, final int total) throws IOException {
      byte[] readBuffer = new byte[total];
      // ensureReadCapacity(total);
      input.readFully(readBuffer, 0, total);
      return readBuffer;
    }

    public static int[] readIntArray(DataInput input, final int len) throws IOException {
      byte[] arr = readBytes(input, len * 4);
      int[] result = new int[len];
      ByteBuffer.wrap(arr).asIntBuffer().get(result);
      return result;
    }

    public static float[] readFloatArray(DataInput input, final int len) throws IOException {
      byte[] arr = readBytes(input, len * 4);
      float[] result = new float[len];
      ByteBuffer.wrap(arr).asFloatBuffer().get(result);
      return result;
    }

    public static double[] readDoubleArray(DataInput input, final int len) throws IOException {
      byte[] arr = readBytes(input, len * 8);
      double[] result = new double[len];
      ByteBuffer.wrap(arr).asDoubleBuffer().get(result);
      return result;
    }

    private static void dumpIntArrayHelper(DataOutput output, int[] arr,
            final int offset, final int len, ByteBuffer byteBuffer)
            throws IOException {
        IntBuffer intBuffer = byteBuffer.asIntBuffer();
        intBuffer.put(arr, offset, len);
        byte[] binary = byteBuffer.array();
        output.write(binary, 0, len * 4);
    }

    private static void dumpFloatArrayHelper(DataOutput output, float[] arr,
            final int offset, final int len, ByteBuffer byteBuffer)
            throws IOException {
        FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
        floatBuffer.put(arr, offset, len);
        byte[] binary = byteBuffer.array();
        output.write(binary, 0, len * 4);
    }

    private static void dumpDoubleArrayHelper(DataOutput output, double[] arr,
            final int offset, final int len, ByteBuffer byteBuffer)
            throws IOException {
        DoubleBuffer doubleBuffer = byteBuffer.asDoubleBuffer();
        doubleBuffer.put(arr, offset, len);
        byte[] binary = byteBuffer.array();
        output.write(binary, 0, len * 8);
    }

    public static void dumpIntArrayStatic(DataOutput output, int[] arr,
            final int offset, final int len) throws IOException {
        dumpIntArrayHelper(output, arr, offset, len, ByteBuffer.allocate(len * 4));
    }

    public static void dumpFloatArrayStatic(DataOutput output, float[] arr,
            final int offset, final int len) throws IOException {
        dumpFloatArrayHelper(output, arr, offset, len, ByteBuffer.allocate(len * 4));
    }

    public static void dumpDoubleArrayStatic(DataOutput output, double[] arr,
            final int offset, final int len) throws IOException {
        dumpDoubleArrayHelper(output, arr, offset, len, ByteBuffer.allocate(len * 8));
    }

    public void dumpIntArray(DataOutput output, int[] arr, final int offset,
            final int len) throws IOException {
        ensureDumpCapacity(len * 4);
        dumpIntArrayHelper(output, arr, offset, len, this.dumpBuffer);
    }

    public void dumpFloatArray(DataOutput output, float[] arr, final int offset,
            final int len) throws IOException {
        ensureDumpCapacity(len * 4);
        dumpFloatArrayHelper(output, arr, offset, len, this.dumpBuffer);
    }

    public void dumpDoubleArray(DataOutput output, double[] arr, final int offset,
            final int len) throws IOException {
        ensureDumpCapacity(len * 8);
        dumpDoubleArrayHelper(output ,arr, offset, len, this.dumpBuffer);
    }
}
