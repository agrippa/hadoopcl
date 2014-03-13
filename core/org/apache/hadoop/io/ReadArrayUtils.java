package org.apache.hadoop.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.nio.IntBuffer;
import java.nio.FloatBuffer;
import java.nio.DoubleBuffer;
import java.nio.ByteBuffer;

public class ReadArrayUtils {
    private ByteBuffer dumpBuffer = null;
    private int dumpLength = -1;

    private byte[] readBuf = new byte[1024];

    private void ensureReadCapacity(final int len) {
        if (len > readBuf.length) {
            int newLength = readBuf.length * 2;
            while (newLength < len) newLength *= 2;
            readBuf = new byte[newLength];
        }
    }

    private void ensureDumpCapacity(final int len) {
        if (this.dumpLength < len) {
            dumpBuffer = ByteBuffer.allocate(len);
            dumpLength = len;
        }
    }

    private byte[] readBytes(final DataInput input, final int total) throws IOException {
        ensureReadCapacity(total);
        input.readFully(readBuf, 0, total);
        return readBuf;
    }

    public static byte[] readBytesStatic(final DataInput input, final int total)
        throws IOException {
      final byte[] readBuffer = new byte[total];
      input.readFully(readBuffer, 0, total);
      return readBuffer;
    }

    public static int[] readIntArray(DataInput input, final int len)
        throws IOException {
      final byte[] arr = readBytesStatic(input, len * 4);
      final int[] result = new int[len];
      ByteBuffer.wrap(arr).asIntBuffer().get(result);
      return result;
    }

    public static float[] readFloatArray(DataInput input, final int len)
        throws IOException {
      final byte[] arr = readBytesStatic(input, len * 4);
      final float[] result = new float[len];
      ByteBuffer.wrap(arr).asFloatBuffer().get(result);
      return result;
    }

    public static double[] readDoubleArray(DataInput input, final int len)
        throws IOException {
      final byte[] arr = readBytesStatic(input, len * 8);
      final double[] result = new double[len];
      ByteBuffer.wrap(arr).asDoubleBuffer().get(result);
      return result;
    }

    public void readIntArrayDynamic(DataInput input, int[] out, int offset, int len)
            throws IOException {
        final byte[] arr = this.readBytes(input, len * 4);
        ByteBuffer.wrap(arr).asIntBuffer().get(out, offset, len);
    }

    public void readFloatArrayDynamic(DataInput input, float[] out, int offset,
            int len) throws IOException {
        final byte[] arr = this.readBytes(input, len * 4);
        ByteBuffer.wrap(arr).asFloatBuffer().get(out, offset, len);
    }

    public void readDoubleArrayDynamic(DataInput input, double[] out, int offset,
            int len) throws IOException {
        final byte[] arr = this.readBytes(input, len * 8);
        ByteBuffer.wrap(arr).asDoubleBuffer().get(out, offset, len);
    }

    public static void readIntArray(DataInput input, int[] out, int offset, int len)
            throws IOException {
        final byte[] arr = readBytesStatic(input, len * 4);
        ByteBuffer.wrap(arr).asIntBuffer().get(out, offset, len);
    }

    public static void readFloatArray(DataInput input, float[] out, int offset,
            int len) throws IOException {
        final byte[] arr = readBytesStatic(input, len * 4);
        ByteBuffer.wrap(arr).asFloatBuffer().get(out, offset, len);
    }

    public static void readDoubleArray(DataInput input, double[] out, int offset,
            int len) throws IOException {
        final byte[] arr = readBytesStatic(input, len * 8);
        ByteBuffer.wrap(arr).asDoubleBuffer().get(out, offset, len);
    }

    private static void dumpIntArrayHelper(DataOutput output, int[] arr,
            final int offset, final int len, ByteBuffer byteBuffer)
            throws IOException {
        final IntBuffer intBuffer = byteBuffer.asIntBuffer();
        intBuffer.put(arr, offset, len);
        final byte[] binary = byteBuffer.array();
        output.write(binary, 0, len * 4);
    }

    private static void dumpFloatArrayHelper(DataOutput output, float[] arr,
            final int offset, final int len, ByteBuffer byteBuffer)
            throws IOException {
        final FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
        floatBuffer.put(arr, offset, len);
        final byte[] binary = byteBuffer.array();
        output.write(binary, 0, len * 4);
    }

    private static void dumpDoubleArrayHelper(DataOutput output, double[] arr,
            final int offset, final int len, ByteBuffer byteBuffer)
            throws IOException {
        final DoubleBuffer doubleBuffer = byteBuffer.asDoubleBuffer();
        doubleBuffer.put(arr, offset, len);
        final byte[] binary = byteBuffer.array();
        output.write(binary, 0, len * 8);
    }

    public static void dumpIntArrayStatic(DataOutput output, int[] arr,
            final int offset, final int len) throws IOException {
        dumpIntArrayHelper(output, arr, offset, len, ByteBuffer.allocate(len * 4));
    }

    public static void dumpFloatArrayStatic(DataOutput output, float[] arr,
            final int offset, final int len) throws IOException {
        dumpFloatArrayHelper(output, arr, offset, len,
            ByteBuffer.allocate(len * 4));
    }

    public static void dumpDoubleArrayStatic(DataOutput output, double[] arr,
            final int offset, final int len) throws IOException {
        dumpDoubleArrayHelper(output, arr, offset, len,
            ByteBuffer.allocate(len * 8));
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
