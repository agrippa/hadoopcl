package org.apache.hadoop.io;

import java.io.IOException;
import java.io.DataInput;
import java.nio.IntBuffer;
import java.nio.FloatBuffer;
import java.nio.DoubleBuffer;
import java.nio.ByteBuffer;

public class ReadArrayUtils {

    public static int[] readIntArray(DataInput input, final int len) throws IOException {
      final int totalBytes = len * 4;
      ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);
      input.readFully(byteBuffer.array(), 0, totalBytes);
      int[] result = new int[len];
      byteBuffer.asIntBuffer().get(result);
      return result;
    }

    public static float[] readFloatArray(DataInput input, final int len) throws IOException {
      final int totalBytes = len * 4;
      ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);
      input.readFully(byteBuffer.array(), 0, totalBytes);
      float[] result = new float[len];
      byteBuffer.asFloatBuffer().get(result);
      return result;
    }

    public static double[] readDoubleArray(DataInput input, final int len) throws IOException {
      final int totalBytes = len * 8;
      ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);
      input.readFully(byteBuffer.array(), 0, totalBytes);
      double[] result = new double[len];
      byteBuffer.asDoubleBuffer().get(result);
      return result;
    }

    private void dumpIntArray(DataOutput output, int[] arr) throws IOException {
        dumpIntArray(output, arr, arr.length);
    }

    private void dumpIntArray(DataOutput output, int[] arr, int len) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(len * 4);
        IntBuffer intBuffer = byteBuffer.asIntBuffer();
        intBuffer.put(arr, 0, len);
        byte[] binary = byteBuffer.array();
        output.write(binary, 0, binary.length);
    }

    private void dumpFloatArray(DataOutput output, float[] arr) throws IOException {
        dumpFloatArray(output, arr, arr.length);
    }

    private void dumpFloatArray(DataOutput output, float[] arr, int len) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(len * 4);
        FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
        floatBuffer.put(arr, 0, len);
        byte[] binary = byteBuffer.array();
        output.write(binary, 0, binary.length);
    }

    private void dumpDoubleArray(DataOutput output, double[] arr) throws IOException {
        dumpDoubleArray(output, arr, arr.length);
    }

    private void dumpDoubleArray(DataOutput output, double[] arr, int len) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(len * 8);
        DoubleBuffer doubleBuffer = byteBuffer.asDoubleBuffer();
        doubleBuffer.put(arr, 0, len);
        byte[] binary = byteBuffer.array();
        output.write(binary, 0, binary.length);
    }

}
