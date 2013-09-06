package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader.BinaryKeyValues;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.*;

import java.lang.Float;
import java.lang.Class;

import com.amd.aparapi.Kernel;
import com.amd.aparapi.Range;
import com.amd.aparapi.device.Device;
import com.amd.aparapi.internal.opencl.OpenCLPlatform;
import com.amd.aparapi.device.OpenCLDevice;

public class OpenCLReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private static final HashSet<String> supportedKeys;
    private static final HashSet<String> supportedValues;
    static {
        supportedKeys = new HashSet<String>();
        supportedValues = new HashSet<String>();
        supportedKeys.add("org.apache.hadoop.io.IntWritable");
        supportedKeys.add("org.apache.hadoop.io.BooleanWritable");
        supportedKeys.add("org.apache.hadoop.io.PairWritable");
        supportedKeys.add("org.apache.hadoop.io.LongWritable");
        supportedValues.add("org.apache.hadoop.io.FloatWritable");
        supportedValues.add("org.apache.hadoop.io.IntWritable");
        supportedValues.add("org.apache.hadoop.io.ArrayPrimitiveWritable");
        supportedValues.add("org.apache.hadoop.io.LongWritable");
        supportedValues.add("org.apache.hadoop.io.PairWritable");
        supportedValues.add("org.apache.hadoop.io.UniquePairWritable");
        supportedValues.add("org.apache.hadoop.io.SparseVectorWritable");
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.err.println("DIAGNOSTICS: Entering OpenCLReducer.run");
        setup(context);
        
        String keyClass = context.getMapOutputKeyClassString();
        String valueClass = context.getMapOutputValueClassString();

        if(!supportedKeys.contains(keyClass)) {
            throw new RuntimeException("Unsupported raw key type "+keyClass);
        }

        if(!supportedValues.contains(valueClass)) {
            throw new RuntimeException("Unsupported raw value type "+valueClass);
        }

        OpenCLDriver driver = null;
        try {
            boolean isCombiner = context.getTaskAttemptID().isMap();
            driver = new OpenCLDriver("reducer", context, isCombiner ? context.getOCLCombinerClass() : context.getOCLReducerClass());
        } catch(java.lang.ClassNotFoundException ce) {
            throw new RuntimeException("Failed to load reducer kernel class");
        }

        driver.run();

        cleanup(context);
    }
}
