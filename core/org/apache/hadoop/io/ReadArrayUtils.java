package org.apache.hadoop.io;

import org.apache.hadoop.fs.FSDataInputStream;
import java.nio.IntBuffer;
import java.nio.FloatBuffer;
import java.nio.DoubleBuffer;
import java.nio.ByteBuffer;

public class ReadArrayUtils {

    public static int[] readIntArray(FSDataInputStream input, final int len) throws IOException {
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

    public static float[] readFloatArray(FSDataInputStream input, final int len) throws IOException {
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

    public static double[] readDoubleArray(FSDataInputStream input, final int len) throws IOException {
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
