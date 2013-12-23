package org.apache.hadoop.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.nio.IntBuffer;
import java.nio.FloatBuffer;
import java.nio.DoubleBuffer;
import java.nio.ByteBuffer;

public class ReadArrayUtils {

    private static byte[] readBytes(DataInput input, final int total) throws IOException {
      byte[] arr = new byte[total];
      input.readFully(arr, 0, total);
      return arr;
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

    public static void dumpIntArray(DataOutput output, int[] arr, final int offset,
            final int len) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(len * 4);
        IntBuffer intBuffer = byteBuffer.asIntBuffer();
        intBuffer.put(arr, offset, len);
        byte[] binary = byteBuffer.array();
        output.write(binary, 0, len * 4);
    }

    public static void dumpFloatArray(DataOutput output, float[] arr, final int offset,
            final int len) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(len * 4);
        FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
        floatBuffer.put(arr, offset, len);
        byte[] binary = byteBuffer.array();
        output.write(binary, 0, len * 4);
    }

    public static void dumpDoubleArray(DataOutput output, double[] arr, final int offset,
            final int len) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(len * 8);
        DoubleBuffer doubleBuffer = byteBuffer.asDoubleBuffer();
        doubleBuffer.put(arr, offset, len);
        byte[] binary = byteBuffer.array();
        output.write(binary, 0, len * 8);
    }
}
