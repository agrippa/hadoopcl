/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.io.KVCollection;
import org.apache.hadoop.mapreduce.BufferRunner;
import org.apache.hadoop.mapreduce.OpenCLDriver;
import org.apache.hadoop.mapreduce.HadoopCLBulkCombinerReader;
import org.apache.hadoop.mapreduce.HadoopCLDataInput;

import org.apache.hadoop.mapreduce.HadoopCLBulkMapperReader;
import org.apache.hadoop.mapreduce.IterateAndPartition;
import org.apache.hadoop.mapreduce.HadoopCLPartitioner;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.HadoopCLKeyValueIterator;
import org.apache.hadoop.mapreduce.DontBlockOnSpillDoneException;
import static org.apache.hadoop.mapred.Task.Counter.COMBINE_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.COMBINE_OUTPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_MATERIALIZED_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.DontBlockOnSpillDoneException;
import org.apache.hadoop.mapreduce.HadoopOpenCLContext;

import java.text.NumberFormat;
import java.text.DecimalFormat;

import com.amd.aparapi.device.OpenCLDevice;
import com.amd.aparapi.device.Device;

/** A Map task. */
public class MapTask extends Task {
  /**
   * The size of each record in the index file for the map-outputs.
   */
  public static int totalIndexCacheMemory = 0;
  private static final NumberFormat numFormat = new DecimalFormat();;
  public static final HashMap<Integer, SpillRecord> indexCacheList = new HashMap<Integer, SpillRecord>();
  // public static final ArrayList<SpillRecord> indexCacheList = new ArrayList<SpillRecord>();
  private static final AtomicInteger numSpills = new AtomicInteger(0);
  public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;
  private Boolean isOpenCLSpiller = null;

  private boolean isOpenCLSpiller() {
    if (isOpenCLSpiller == null) {
      boolean result = false;
      try {
        if (taskContext.getMapperClass().getName().equals("org.apache.hadoop.mapreduce.OpenCLMapper")) {
          final String deviceStr = System.getProperty("opencl.device");
          if (deviceStr != null) {
            final OpenCLDevice dev = HadoopOpenCLContext.findDevice(Integer.parseInt(deviceStr));
            result = dev.getType() == Device.TYPE.GPU ||
                dev.getType() == Device.TYPE.CPU;
          }
        }
      } catch(Exception e) { }
      isOpenCLSpiller = new Boolean(result);
    }
    return isOpenCLSpiller.booleanValue();
  }

  private TaskSplitIndex splitMetaInfo = new TaskSplitIndex();
  private final static int APPROX_HEADER_LENGTH = 150;

  private static final Log LOG = LogFactory.getLog(MapTask.class.getName());

  {   // set phase for this task
    setPhase(TaskStatus.Phase.MAP); 
  }

  public MapTask() {
    super();
  }

  public MapTask(String jobFile, TaskAttemptID taskId, 
                 int partition, TaskSplitIndex splitIndex,
                 int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.splitMetaInfo = splitIndex;
  }

  @Override
  public boolean isMapTask() {
    return true;
  }

  @Override
  public void localizeConfiguration(JobConf conf)
      throws IOException {
    super.localizeConfiguration(conf);
    // split.info file is used only by IsolationRunner.
    // Write the split file to the local disk if it is a normal map task (not a
    // job-setup or a job-cleanup task) and if the user wishes to run
    // IsolationRunner either by setting keep.failed.tasks.files to true or by
    // using keep.tasks.files.pattern
    if (supportIsolationRunner(conf) && isMapOrReduce()) {
      // localize the split meta-information
      Path localSplitMeta =
        new LocalDirAllocator("mapred.local.dir").getLocalPathForWrite(
            TaskTracker.getLocalSplitFile(conf.getUser(), getJobID()
                .toString(), getTaskID().toString()), conf);
      LOG.debug("Writing local split to " + localSplitMeta);
      DataOutputStream out = FileSystem.getLocal(conf).create(localSplitMeta);
      splitMetaInfo.write(out);
      out.close();
    }
  }
  
  @Override
  public TaskRunner createRunner(TaskTracker tracker, 
                                 TaskTracker.TaskInProgress tip,
                                 TaskTracker.RunningJob rjob
                                 ) throws IOException {
    return new MapTaskRunner(tip, tracker, this.conf, rjob);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    if (isMapOrReduce()) {
      if (splitMetaInfo != null) {
        splitMetaInfo.write(out);
      } else {
        new TaskSplitIndex().write(out);
      }
      //TODO do we really need to set this to null?
      splitMetaInfo = null;
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    if (isMapOrReduce()) {
      splitMetaInfo.readFields(in);
    }
  }

  /**
   * This class wraps the user's record reader to update the counters and
   * progress as records are read.
   * @param <K>
   * @param <V>
   */
  class TrackedRecordReader<K, V> 
      implements RecordReader<K,V> {
    private RecordReader<K,V> rawIn;
    private Counters.Counter inputByteCounter;
    private Counters.Counter inputRecordCounter;
    private Counters.Counter fileInputByteCounter;
    private InputSplit split;
    private TaskReporter reporter;
    private long beforePos = -1;
    private long afterPos = -1;
    private long bytesInPrev = -1;
    private long bytesInCurr = -1;
    private final Statistics fsStats;

    TrackedRecordReader(InputSplit split, JobConf job, TaskReporter reporter)
        throws IOException {
      inputRecordCounter = reporter.getCounter(MAP_INPUT_RECORDS);
      inputByteCounter = reporter.getCounter(MAP_INPUT_BYTES);
      fileInputByteCounter = reporter
          .getCounter(FileInputFormat.Counter.BYTES_READ);

      Statistics matchedStats = null;
      if (split instanceof FileSplit) {
        matchedStats = getFsStatistics(((FileSplit) split).getPath(), job);
      } 
      fsStats = matchedStats;
      
      bytesInPrev = getInputBytes(fsStats);
      rawIn = job.getInputFormat().getRecordReader(split, job, reporter);
      bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
      
      this.reporter = reporter;
      this.split = split;
      conf = job;
    }

    public K createKey() {
      return rawIn.createKey();
    }
      
    public V createValue() {
      return rawIn.createValue();
    }
     
    public synchronized boolean next(K key, V value)
    throws IOException {
      boolean ret = moveToNext(key, value);      
      if (ret) {
        incrCounters();
      }
      return ret;
    }
    
    protected void incrCounters() {
      inputRecordCounter.increment(1);
      inputByteCounter.increment(afterPos - beforePos);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }
     
    protected synchronized boolean moveToNext(K key, V value)
      throws IOException {
      boolean ret = false;
      try {
        reporter.setProgress(getProgress());
        beforePos = getPos();
        bytesInPrev = getInputBytes(fsStats);
        ret = rawIn.next(key, value);
        afterPos = getPos();
        bytesInCurr = getInputBytes(fsStats);
      } catch (IOException ioe) {
        if (split instanceof FileSplit) {
          LOG.error("IO error in map input file " + conf.get("map.input.file"));
          throw new IOException("IO error in map input file "
              + conf.get("map.input.file"), ioe);
        }
        throw ioe;
      }
      return ret;
    }

    public long getPos() throws IOException { return rawIn.getPos(); }
    
    public void close() throws IOException {
      bytesInPrev = getInputBytes(fsStats);
      rawIn.close(); 
      bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }
    
    public float getProgress() throws IOException {
      return rawIn.getProgress();
    }
    TaskReporter getTaskReporter() {
      return reporter;
    }
    
    private long getInputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesRead();
    }
  }

  /**
   * This class skips the records based on the failed ranges from previous 
   * attempts.
   */
  class SkippingRecordReader<K, V> extends TrackedRecordReader<K,V> {
    private SkipRangeIterator skipIt;
    private SequenceFile.Writer skipWriter;
    private boolean toWriteSkipRecs;
    private TaskUmbilicalProtocol umbilical;
    private Counters.Counter skipRecCounter;
    private long recIndex = -1;
    
    SkippingRecordReader(InputSplit split, TaskUmbilicalProtocol umbilical,
                         TaskReporter reporter) throws IOException {
      super(split, conf, reporter);
      this.umbilical = umbilical;
      this.skipRecCounter = reporter.getCounter(Counter.MAP_SKIPPED_RECORDS);
      this.toWriteSkipRecs = toWriteSkipRecs() &&  
          SkipBadRecords.getSkipOutputPath(conf)!=null;
      skipIt = getSkipRanges().skipRangeIterator();
    }
    
    public synchronized boolean next(K key, V value)
    throws IOException {
      if(!skipIt.hasNext()) {
        LOG.warn("Further records got skipped.");
        return false;
      }
      boolean ret = moveToNext(key, value);
      long nextRecIndex = skipIt.next();
      long skip = 0;
      while(recIndex<nextRecIndex && ret) {
        if(toWriteSkipRecs) {
          writeSkippedRec(key, value);
        }
      	ret = moveToNext(key, value);
        skip++;
      }
      //close the skip writer once all the ranges are skipped
      if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
        skipWriter.close();
      }
      skipRecCounter.increment(skip);
      reportNextRecordRange(umbilical, recIndex);
      if (ret) {
        incrCounters();
      }
      return ret;
    }
    
    protected synchronized boolean moveToNext(K key, V value)
    throws IOException {
	    recIndex++;
      return super.moveToNext(key, value);
    }
    
    @SuppressWarnings("unchecked")
    private void writeSkippedRec(K key, V value) throws IOException{
      if(skipWriter==null) {
        Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
        Path skipFile = new Path(skipDir, getTaskID().toString());
        skipWriter = 
          SequenceFile.createWriter(
              skipFile.getFileSystem(conf), conf, skipFile,
              (Class<K>) createKey().getClass(),
              (Class<V>) createValue().getClass(), 
              CompressionType.BLOCK, getTaskReporter());
      }
      skipWriter.append(key, value);
    }
  }

  @Override
  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical) 
    throws IOException, ClassNotFoundException, InterruptedException {
    this.umbilical = umbilical;

    // start thread that will handle communication with parent
    TaskReporter reporter = new TaskReporter(getProgress(), umbilical,
        jvmContext);
    reporter.startCommunicationThread();
    boolean useNewApi = job.getUseNewMapper();
    initialize(job, getJobID(), reporter, useNewApi);

    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }

    if (useNewApi) {
      LOG.info("Xiangyu: " + System.currentTimeMillis() + " mapper starts");
      runNewMapper(job, splitMetaInfo, umbilical, reporter);
      LOG.info("Xiangyu: " + System.currentTimeMillis() + " mapper ends");
    } else {
      runOldMapper(job, splitMetaInfo, umbilical, reporter);
    }
    done(umbilical, reporter);
  }
  @SuppressWarnings("unchecked")
  private <T> T getSplitDetails(Path file, long offset)
   throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    FSDataInputStream inFile = fs.open(file);
    inFile.seek(offset);
    String className = Text.readString(inFile);
    Class<T> cls;
    try {
      cls = (Class<T>) conf.getClassByName(className);
    } catch (ClassNotFoundException ce) {
      IOException wrap = new IOException("Split class " + className +
                                          " not found");
      wrap.initCause(ce);
      throw wrap;
    }
    SerializationFactory factory = new SerializationFactory(conf);
    Deserializer<T> deserializer =
      (Deserializer<T>) factory.getDeserializer(cls);
    deserializer.open(inFile);
    T split = deserializer.deserialize(null);
    long pos = inFile.getPos();
    getCounters().findCounter(
         Task.Counter.SPLIT_RAW_BYTES).increment(pos - offset);
    inFile.close();
    return split;
  }
  
  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runOldMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, InterruptedException,
                             ClassNotFoundException {
    InputSplit inputSplit = getSplitDetails(new Path(splitIndex.getSplitLocation()),
           splitIndex.getStartOffset());

    updateJobWithSplit(job, inputSplit);
    reporter.setInputSplit(inputSplit);

    RecordReader<INKEY,INVALUE> in = isSkipping() ? 
        new SkippingRecordReader<INKEY,INVALUE>(inputSplit, umbilical, reporter) :
        new TrackedRecordReader<INKEY,INVALUE>(inputSplit, job, reporter);
    job.setBoolean("mapred.skip.on", isSkipping());


    int numReduceTasks = conf.getNumReduceTasks();
    LOG.info("numReduceTasks: " + numReduceTasks);
    MapOutputCollector collector = null;
    if (numReduceTasks > 0) {
      collector = new MapOutputBuffer(umbilical, job, reporter);
    } else { 
      collector = new DirectMapOutputCollector(umbilical, job, reporter);
    }
    MapRunnable<INKEY,INVALUE,OUTKEY,OUTVALUE> runner =
      ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

    try {
      runner.run(in, new OldOutputCollector(collector, conf), reporter);
      collector.flush();
    } finally {
      //close
      in.close();                               // close input
      collector.close();
    }
  }

  /**
   * Update the job with details about the file split
   * @param job the job configuration to update
   * @param inputSplit the file split
   */
  private void updateJobWithSplit(final JobConf job, InputSplit inputSplit) {
    if (inputSplit instanceof FileSplit) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      job.set("map.input.file", fileSplit.getPath().toString());
      job.setLong("map.input.start", fileSplit.getStart());
      job.setLong("map.input.length", fileSplit.getLength());
    }
  }

  public static class NewTrackingRecordReader<K,V> 
    extends org.apache.hadoop.mapreduce.RecordReader<K,V> {
    public final org.apache.hadoop.mapreduce.RecordReader<K,V> real;
    private final org.apache.hadoop.mapreduce.Counter inputRecordCounter;
    private final org.apache.hadoop.mapreduce.Counter fileInputByteCounter;
    private final TaskReporter reporter;
    private org.apache.hadoop.mapreduce.InputSplit inputSplit;
    private final JobConf job;
    private final Statistics fsStats;
    
    NewTrackingRecordReader(org.apache.hadoop.mapreduce.InputSplit split,
        org.apache.hadoop.mapreduce.InputFormat inputFormat,
        TaskReporter reporter, JobConf job,
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext)
        throws IOException, InterruptedException {
      this.reporter = reporter;
      this.inputSplit = split;
      this.job = job;
      this.inputRecordCounter = reporter.getCounter(MAP_INPUT_RECORDS);
      this.fileInputByteCounter = reporter
          .getCounter(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.Counter.BYTES_READ);

      Statistics matchedStats = null;
      if (split instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
        matchedStats = getFsStatistics(((org.apache.hadoop.mapreduce.lib.input.FileSplit) split)
            .getPath(), job);
      } 
      fsStats = matchedStats;
	  
      long bytesInPrev = getInputBytes(fsStats);
      this.real = inputFormat.createRecordReader(split, taskContext);
      long bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    public org.apache.hadoop.mapreduce.RecordReader getReal() {
        return real;
    }

    @Override
    public void close() throws IOException {
      long bytesInPrev = getInputBytes(fsStats);
      real.close();
      long bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
      return real.getCurrentKey();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
      return real.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return real.getProgress();
    }

    @Override
    public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
                           org.apache.hadoop.mapreduce.TaskAttemptContext context
                           ) throws IOException, InterruptedException {
      long bytesInPrev = getInputBytes(fsStats);
      real.initialize(split, context);
      long bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      boolean result = false;
      try {
        long bytesInPrev = getInputBytes(fsStats);
        result = real.nextKeyValue();
        long bytesInCurr = getInputBytes(fsStats);

        if (result) {
          inputRecordCounter.increment(1);
          fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
        }
        reporter.setProgress(getProgress());
      } catch (IOException ioe) {
        if (inputSplit instanceof FileSplit) {
          FileSplit fileSplit = (FileSplit) inputSplit;
          LOG.error("IO error in map input file "
              + fileSplit.getPath().toString());
          throw new IOException("IO error in map input file "
              + fileSplit.getPath().toString(), ioe);
        }
        throw ioe;
      }
      return result;
    }
    
    private long getInputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesRead();
    }

    public boolean supportsBulkReads() {
        return real.supportsBulkReads();
    }
  }

  /**
   * Since the mapred and mapreduce Partitioners don't share a common interface
   * (JobConfigurable is deprecated and a subtype of mapred.Partitioner), the
   * partitioner lives in Old/NewOutputCollector. Note that, for map-only jobs,
   * the configured partitioner should not be called. It's common for
   * partitioners to compute a result mod numReduces, which causes a div0 error
   */
  private static class OldOutputCollector<K,V> implements OutputCollector<K,V> {
    private final Partitioner<K,V> partitioner;
    private final MapOutputCollector<K,V> collector;
    private final int numPartitions;

    @SuppressWarnings("unchecked")
    OldOutputCollector(MapOutputCollector<K,V> collector, JobConf conf) {
      numPartitions = conf.getNumReduceTasks();
      if (numPartitions > 0) {
        partitioner = (Partitioner<K,V>)
          ReflectionUtils.newInstance(conf.getPartitionerClass(), conf);
      } else {
        partitioner = new Partitioner<K,V>() {
          @Override
          public void configure(JobConf job) { }
          @Override
          public int getPartition(K key, V value, int numPartitions) {
            return -1;
          }
        };
      }
      this.collector = collector;
    }

    @Override
    public void collect(K key, V value) throws IOException {
      try {
        collector.collect(key, value,
                          partitioner.getPartition(key, value, numPartitions));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupt exception", ie);
      }
    }

    @Override
    public int collectCollection(KVCollection<K, V> coll) {
        throw new UnsupportedOperationException();
    }
  }

  private class NewDirectOutputCollector<K,V>
  extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final org.apache.hadoop.mapreduce.RecordWriter out;

    private final TaskReporter reporter;

    private final Counters.Counter mapOutputRecordCounter;
    private final Counters.Counter fileOutputByteCounter; 
    private final Statistics fsStats;
    
    @SuppressWarnings("unchecked")
    NewDirectOutputCollector(org.apache.hadoop.mapreduce.JobContext jobContext,
        JobConf job, TaskUmbilicalProtocol umbilical, TaskReporter reporter) 
    throws IOException, ClassNotFoundException, InterruptedException {
      this.reporter = reporter;
      Statistics matchedStats = null;
      if (outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
        matchedStats = getFsStatistics(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
            .getOutputPath(jobContext), job);
      }
      fsStats = matchedStats;
      mapOutputRecordCounter = 
        reporter.getCounter(MAP_OUTPUT_RECORDS);
      fileOutputByteCounter = reporter
          .getCounter(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.Counter.BYTES_WRITTEN);

      long bytesOutPrev = getOutputBytes(fsStats);
      out = outputFormat.getRecordWriter(taskContext);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(K key, V value) 
    throws IOException, InterruptedException {
      reporter.progress();
      long bytesOutPrev = getOutputBytes(fsStats);
      out.write(key, value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      mapOutputRecordCounter.increment(1);
    }

    @Override
    public int writeCollection(KVCollection<K,V> coll) throws IOException, InterruptedException {
        if (out != null) {
            return out.writeCollection(coll);
        } else {
            return -1;
        }
    }

    @Override
    public void writeChunk(byte[] arr, int len) {
        throw new java.lang.UnsupportedOperationException("Do not support writeChunk in mapred.MapTask.NewDirectOutputCollector");
    }

    @Override
    public void close(TaskAttemptContext context) 
    throws IOException,InterruptedException {
      reporter.progress();
      if (out != null) {
        long bytesOutPrev = getOutputBytes(fsStats);
        out.close(context);
        long bytesOutCurr = getOutputBytes(fsStats);
        fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      }
    }

    private long getOutputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesWritten();
    }
  }
  
  private class NewOutputCollector<K extends Comparable<K> & Writable, V extends Comparable<V> & Writable>
    extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final MapOutputCollector<K,V> collector;
    private final org.apache.hadoop.mapreduce.Partitioner<K,V> partitioner;
    private final int partitions;

    @SuppressWarnings("unchecked")
    NewOutputCollector(org.apache.hadoop.mapreduce.JobContext jobContext,
                       JobConf job,
                       TaskUmbilicalProtocol umbilical,
                       TaskReporter reporter
                       ) throws IOException, ClassNotFoundException {
      collector = new MapOutputBuffer<K,V>(umbilical, job, reporter);
      partitions = jobContext.getNumReduceTasks();
      if (partitions > 0) {
        partitioner = (org.apache.hadoop.mapreduce.Partitioner<K,V>)
          ReflectionUtils.newInstance(jobContext.getPartitionerClass(), job);
      } else {
        partitioner = new org.apache.hadoop.mapreduce.Partitioner<K,V>() {
          @Override
          public int getPartition(K key, V value, int numPartitions) {
            return -1;
          }
        };
      }
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      collector.collect(key, value,
                        partitioner.getPartition(key, value, partitions));
    }

    @Override
    public int writeCollection(KVCollection<K,V> coll) throws IOException, InterruptedException {
        return collector.collectCollection(coll, partitioner);
    }

    @Override
    public void writeChunk(byte[] arr, int len) {
        throw new java.lang.UnsupportedOperationException("Do not support writeChunk in mapred.MapTask.NewOutputCollector");
    }

    @Override
    public void close(TaskAttemptContext context
                      ) throws IOException,InterruptedException {
      try {
        collector.flush();
      } catch (ClassNotFoundException cnf) {
        throw new IOException("can't find class ", cnf);
      }
      collector.close();
    }
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, ClassNotFoundException,
                             InterruptedException {
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.TaskAttemptContext(job, getTaskID());
    // make a mapper
    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper =
      (org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getMapperClass(), job);
    // make the input format
    org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE> inputFormat =
      (org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE>)
        ReflectionUtils.newInstance(taskContext.getInputFormatClass(), job);
    // rebuild the input split
    org.apache.hadoop.mapreduce.InputSplit split = null;
    split = getSplitDetails(new Path(splitIndex.getSplitLocation()),
        splitIndex.getStartOffset());

    org.apache.hadoop.mapreduce.RecordReader<INKEY,INVALUE> input =
      new NewTrackingRecordReader<INKEY,INVALUE>
          (split, inputFormat, reporter, job, taskContext);

    job.setBoolean("mapred.skip.on", isSkipping());
    org.apache.hadoop.mapreduce.RecordWriter output = null;
    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context 
         mapperContext = null;
    try {
      Constructor<org.apache.hadoop.mapreduce.Mapper.Context> contextConstructor =
        org.apache.hadoop.mapreduce.Mapper.Context.class.getConstructor
        (new Class[]{org.apache.hadoop.mapreduce.Mapper.class,
                     Configuration.class,
                     org.apache.hadoop.mapreduce.TaskAttemptID.class,
                     org.apache.hadoop.mapreduce.RecordReader.class,
                     org.apache.hadoop.mapreduce.RecordWriter.class,
                     org.apache.hadoop.mapreduce.OutputCommitter.class,
                     org.apache.hadoop.mapreduce.StatusReporter.class,
                     org.apache.hadoop.mapreduce.InputSplit.class});

      // get an output object
      if (job.getNumReduceTasks() == 0) {
         output =
           new NewDirectOutputCollector(taskContext, job, umbilical, reporter);
      } else {
        output = new NewOutputCollector(taskContext, job, umbilical, reporter);
      }

      mapperContext = contextConstructor.newInstance(mapper, job, getTaskID(),
                                                     input, output, committer,
                                                     reporter, split);

      input.initialize(split, mapperContext);
      mapper.run(mapperContext);
      input.close();
      output.close(mapperContext);
    } catch (NoSuchMethodException e) {
      throw new IOException("Can't find Context constructor", e);
    } catch (InstantiationException e) {
      throw new IOException("Can't create Context", e);
    } catch (InvocationTargetException e) {
      throw new IOException("Can't invoke Context constructor", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Can't invoke Context constructor", e);
    }
  }

  interface MapOutputCollector<K, V> {

    public void collect(K key, V value, int partition
                        ) throws IOException, InterruptedException;
    public int collectCollection(KVCollection<K,V> kv,
        org.apache.hadoop.mapreduce.Partitioner<K,V> partitioner)
          throws IOException, InterruptedException;
    public void close() throws IOException, InterruptedException;
    
    public void flush() throws IOException, InterruptedException, 
                               ClassNotFoundException;
        
  }

  class DirectMapOutputCollector<K, V>
    implements MapOutputCollector<K, V> {
 
    private RecordWriter<K, V> out = null;

    private TaskReporter reporter = null;

    private final Counters.Counter mapOutputRecordCounter;
    private final Counters.Counter fileOutputByteCounter;
    private final Statistics fsStats;

    @SuppressWarnings("unchecked")
    public DirectMapOutputCollector(TaskUmbilicalProtocol umbilical,
        JobConf job, TaskReporter reporter) throws IOException {
      this.reporter = reporter;
      String finalName = getOutputName(getPartition());
      FileSystem fs = FileSystem.get(job);

      
      OutputFormat<K, V> outputFormat = job.getOutputFormat();
      
      Statistics matchedStats = null;
      if (outputFormat instanceof FileOutputFormat) {
        matchedStats = getFsStatistics(FileOutputFormat.getOutputPath(job), job);
      } 
      fsStats = matchedStats;
      mapOutputRecordCounter = reporter.getCounter(MAP_OUTPUT_RECORDS);
      fileOutputByteCounter = reporter
          .getCounter(FileOutputFormat.Counter.BYTES_WRITTEN);
      
      long bytesOutPrev = getOutputBytes(fsStats);
      out = job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    public void close() throws IOException {
      if (this.out != null) {
        long bytesOutPrev = getOutputBytes(fsStats);
        out.close(this.reporter);
        long bytesOutCurr = getOutputBytes(fsStats);
        fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      }

    }

    public void flush() throws IOException, InterruptedException, 
                               ClassNotFoundException {
    }

    public int collectCollection(KVCollection<K,V> coll,
        org.apache.hadoop.mapreduce.Partitioner<K,V> partitioner)
          throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    public void collect(K key, V value, int partition) throws IOException {
      reporter.progress();
      long bytesOutPrev = getOutputBytes(fsStats);
      out.write(key, value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      mapOutputRecordCounter.increment(1);
    }
    
    private long getOutputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesWritten();
    }
  }

  public class MapOutputBuffer<K extends Comparable<K> & Writable, V extends Comparable<V> & Writable> 
      implements MapOutputCollector<K, V>, IndexedSortable {
    private final int partitions;
    private final JobConf job;
    private final TaskReporter reporter;
    private final Class<K> keyClass;
    private final Class<V> valClass;
    private final RawComparator<K> comparator;
    private final SerializationFactory serializationFactory;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valSerializer;
    private final CombinerRunner<K,V> combinerRunner;
    private final CombineOutputCollector<K, V> combineCollector;
    private final CombineOutputCollector<K, V> flushCombineCollector;
    private boolean alreadyReleased = true;
    
    // Compression for map-outputs
    private CompressionCodec codec = null;

    // k/v accounting
    private volatile int kvstart = 0;  // marks beginning of spill
    private volatile int kvend = 0;    // marks beginning of collectable
    private int kvindex = 0;           // marks end of collected
    private final int[] kvoffsets;     // indices into kvindices
    private final int[] kvindices;     // partition, k/v offsets into kvbuffer
    private volatile int bufstart = 0; // marks beginning of spill
    private volatile int bufend = 0;   // marks beginning of collectable
    private volatile int bufvoid = 0;  // marks the point where we should stop
                                       // reading at the end of the buffer
    private int bufindex = 0;          // marks end of collected
    private int bufmark = 0;           // marks end of record
    private byte[] kvbuffer;           // main output buffer
    private static final int PARTITION = 0; // partition offset in acct
    public static final int KEYSTART = 1;  // key offset in acct
    public static final int VALSTART = 2;  // val offset in acct
    public static final int ACCTSIZE = 3;  // total #fields in acct
    public static final int RECSIZE =
                       (ACCTSIZE + 1) * 4;  // acct bytes per record

    // spill accounting
    // private volatile int numSpills = 0;
    private final float spillper;
    private final float initialspillper;
    private volatile Throwable sortSpillException = null;
    private int softRecordLimit;
    private int softBufferLimit;
    private final float quickRestartPercent;
    private final float maxRestartPercent;
    private int minSpillsForCombine;
    private final IndexedSorter sorter;
    private final ReentrantLock spillLock = new ReentrantLock();
    private final Condition spillDone = spillLock.newCondition();
    private final Condition spillReady = spillLock.newCondition();
    private final BlockingBuffer bb = new BlockingBuffer();
    private volatile boolean spillRunning = false;
    private volatile boolean spillThreadRunning = false;
    private volatile boolean noMoreSpilling = false;
    private final SpillThread spillThread = new SpillThread();

    private final FileSystem localFs;
    private final FileSystem rfs;
   
    private final Counters.Counter mapOutputByteCounter;
    private final Counters.Counter mapOutputRecordCounter;
    private final Counters.Counter combineOutputCounter;
    private final Counters.Counter fileOutputByteCounter;
   
    // private ArrayList<SpillRecord> indexCacheList;
    // private int totalIndexCacheMemory;
    private static final int INDEX_CACHE_MEMORY_LIMIT = 1024 * 1024;

    @SuppressWarnings("unchecked")
    public MapOutputBuffer(TaskUmbilicalProtocol umbilical, JobConf job,
                           TaskReporter reporter
                           ) throws IOException, ClassNotFoundException {
      this.job = job;
      this.reporter = reporter;
      localFs = FileSystem.getLocal(job);
      partitions = job.getNumReduceTasks();
       
      rfs = ((LocalFileSystem)localFs).getRaw();

      // indexCacheList = new ArrayList<SpillRecord>();
      
      //sanity checks
      if (isOpenCLSpiller()) {
          spillper = job.getFloat("io.sort.spill.percent.hadoopcl",(float)0.8);
          initialspillper = job.getFloat("io.sort.spill.percent.hadoopcl.initial", (float)0.4);
      } else {
          spillper = job.getFloat("io.sort.spill.percent",(float)0.8);
          initialspillper = spillper;
      }

      maxRestartPercent = job.getFloat("io.sort.maxrestart", (float)0.5);
      quickRestartPercent = job.getFloat("io.sort.quickrestart", (float)0.3);
      final float recper = job.getFloat("io.sort.record.percent",(float)0.05);
      final int sortmb = job.getInt("io.sort.mb", 100);
      if (spillper > (float)1.0 || spillper < (float)0.0) {
        throw new IOException("Invalid \"io.sort.spill.percent\": " + spillper);
      }
      if (recper > (float)1.0 || recper < (float)0.01) {
        throw new IOException("Invalid \"io.sort.record.percent\": " + recper);
      }
      if ((sortmb & 0x7FF) != sortmb) {
        throw new IOException("Invalid \"io.sort.mb\": " + sortmb);
      }
      sorter = ReflectionUtils.newInstance(
            job.getClass("map.sort.class", QuickSort.class, IndexedSorter.class), job);
      LOG.info("io.sort.mb = " + sortmb);
      // buffers and accounting
      int maxMemUsage = sortmb << 20;
      int recordCapacity = (int)(maxMemUsage * recper);
      recordCapacity -= recordCapacity % RECSIZE;
      kvbuffer = new byte[maxMemUsage - recordCapacity];
      bufvoid = kvbuffer.length;
      recordCapacity /= RECSIZE;
      kvoffsets = new int[recordCapacity];
      kvindices = new int[recordCapacity * ACCTSIZE];
      // softBufferLimit = (int)(kvbuffer.length * spillper);
      // softRecordLimit = (int)(kvoffsets.length * spillper);
      softBufferLimit = (int)(kvbuffer.length * initialspillper);
      softRecordLimit = (int)(kvoffsets.length * initialspillper);
      LOG.info("data buffer = " + softBufferLimit + "/" + kvbuffer.length);
      LOG.info("record buffer = " + softRecordLimit + "/" + kvoffsets.length);
      // k/v serialization
      comparator = job.getOutputKeyComparator();
      keyClass = (Class<K>)job.getMapOutputKeyClass();
      valClass = (Class<V>)job.getMapOutputValueClass();
      serializationFactory = new SerializationFactory(job);
      keySerializer = serializationFactory.getSerializer(keyClass);
      keySerializer.open(bb);
      valSerializer = serializationFactory.getSerializer(valClass);
      valSerializer.open(bb);
      // counters
      mapOutputByteCounter = reporter.getCounter(MAP_OUTPUT_BYTES);
      mapOutputRecordCounter = reporter.getCounter(MAP_OUTPUT_RECORDS);
      Counters.Counter combineInputCounter = 
        reporter.getCounter(COMBINE_INPUT_RECORDS);
      combineOutputCounter = reporter.getCounter(COMBINE_OUTPUT_RECORDS);
      fileOutputByteCounter = reporter.getCounter(MAP_OUTPUT_MATERIALIZED_BYTES);
      // compression
      if (job.getCompressMapOutput()) {
        Class<? extends CompressionCodec> codecClass =
          job.getMapOutputCompressorClass(DefaultCodec.class);
        codec = ReflectionUtils.newInstance(codecClass, job);
      }
      // combiner
      combinerRunner = CombinerRunner.create(job, getTaskID(), 
                                             combineInputCounter,
                                             reporter, null, this);
      if (combinerRunner != null) {
        combineCollector= new CombineOutputCollector<K,V>(combineOutputCounter, reporter, conf);
        flushCombineCollector= new CombineOutputCollector<K,V>(combineOutputCounter, reporter, conf);
      } else {
        combineCollector = null;
        flushCombineCollector = null;
      }
      minSpillsForCombine = job.getInt("min.num.spills.for.combine", 3);
      spillThread.setDaemon(true);
      spillThread.setName("SpillThread");
      spillLock.lock();
      try {
        spillThread.start();
        while (!spillThreadRunning) {
          spillDone.await();
        }
      } catch (InterruptedException e) {
        throw (IOException)new IOException("Spill thread failed to initialize"
            ).initCause(sortSpillException);
      } finally {
        spillLock.unlock();
      }
      if (sortSpillException != null) {
        throw (IOException)new IOException("Spill thread failed to initialize"
            ).initCause(sortSpillException);
      }
    }

    public synchronized int collectCollection(KVCollection<K,V> coll,
        org.apache.hadoop.mapreduce.Partitioner<K,V> partitioner)
          throws IOException, InterruptedException {
      reporter.progress();

      spillLock.lock();
      int kvnext = (kvindex + 1) % kvoffsets.length;
      try {
        boolean kvfull;
        do {
          if (sortSpillException != null) {
            throw (IOException)new IOException("Spill failed"
                ).initCause(sortSpillException);
          }
          // sufficient acct space
          kvfull = kvnext == kvstart;
          final boolean kvsoftlimit = ((kvnext > kvend)
              ? kvnext - kvend > softRecordLimit
              : kvend - kvnext <= kvoffsets.length - softRecordLimit);
          if (kvsoftlimit && !spillRunning /* kvstart == kvend */ ) {
            LOG.info("Spilling map output: record full = " + kvsoftlimit);
            startSpill();
          }
          if (kvfull) {
            if (isOpenCLSpiller()) {
                return coll.start();
            }
            try {
              if (OpenCLDriver.logger != null) {
                  // LOG:PROFILE
                  // OpenCLDriver.logger.log("Blocking in collect", "mapper");
              }
              while (kvnext == kvstart) {
                reporter.progress();
                spillDone.await();
              }
              if (OpenCLDriver.logger != null) {
                  // LOG:PROFILE
                  // OpenCLDriver.logger.log("Unblocking in collect", "mapper");
              }
            } catch (InterruptedException e) {
              throw (IOException)new IOException(
                  "Collector interrupted while waiting for the writer"
                  ).initCause(e);
            }
          }
        } while (kvfull);
      } finally {
        spillLock.unlock();
      }

      int oldKvIndex = kvindex;
      int oldBufIndex = bufindex;
      int oldBufStart = bufstart;
      int oldBufEnd = bufend;
      int index = coll.start();

      for ( ; index < coll.end() &&
              ((kvindex + 1) % kvoffsets.length) != kvstart; index++) {
        if (!coll.isValid(index)) continue;

        final int partition = coll.getPartitionFor(index, partitions);
        try {
              kvnext = (kvindex + 1) % kvoffsets.length;
              int keystart = bufindex;
              coll.serializeKey(index, bb);
              if (bufindex < keystart) {
                // wrapped the key; reset required
                bb.reset();
                keystart = 0;
              }
              final int valstart = bufindex;
              coll.serializeValue(index, bb);
              final int valend = bb.markRecord();

              mapOutputRecordCounter.increment(1);
              mapOutputByteCounter.increment(valend >= keystart
                  ? valend - keystart
                  : (bufvoid - keystart) + valend);

              // update accounting info
              int ind = kvindex * ACCTSIZE;
              kvoffsets[kvindex] = ind;
              kvindices[ind + PARTITION] = partition;
              kvindices[ind + KEYSTART] = keystart;
              kvindices[ind + VALSTART] = valstart;
              kvindex = kvnext;
        } catch (MapBufferTooSmallException e) {
          LOG.info("Record too large for in-memory buffer: " + e.getMessage());
          spillSingleRecord((K)coll.getKeyFor(index, null), (V)coll.getValueFor(index, null), partition);
          mapOutputRecordCounter.increment(1);
        } catch (DontBlockOnSpillDoneException db) {
          return index;
        }
      }

      spillLock.lock();
      try {
          final boolean kvsoftlimit = ((kvnext > kvend)
              ? kvnext - kvend > softRecordLimit
              : kvend - kvnext <= kvoffsets.length - softRecordLimit);
          if (kvsoftlimit && !spillRunning /* kvstart == kvend */ ) {
            LOG.info("Spilling map output: record full = " + kvsoftlimit);
            startSpill();
          }
      } finally {
          spillLock.unlock();
      }

      if (index == coll.end()) return -1;
      else return index;
    }

    public synchronized void collect(K key, V value, int partition
                                     ) throws IOException {
      reporter.progress();
      if (key.getClass() != keyClass) {
        throw new IOException("Type mismatch in key from map: expected "
                              + keyClass.getName() + ", recieved "
                              + key.getClass().getName());
      }
      if (value.getClass() != valClass) {
        throw new IOException("Type mismatch in value from map: expected "
                              + valClass.getName() + ", recieved "
                              + value.getClass().getName());
      }
      final int kvnext = (kvindex + 1) % kvoffsets.length;
      spillLock.lock();
      try {
        boolean kvfull;
        do {
          if (sortSpillException != null) {
            throw (IOException)new IOException("Spill failed"
                ).initCause(sortSpillException);
          }
          // sufficient acct space
          kvfull = kvnext == kvstart;
          final boolean kvsoftlimit = ((kvnext > kvend)
              ? kvnext - kvend > softRecordLimit
              : kvend - kvnext <= kvoffsets.length - softRecordLimit);
          if (kvsoftlimit && kvstart == kvend) {
            LOG.info("Spilling map output: record full = " + kvsoftlimit);
            startSpill();
          }
          if (kvfull) {
            // if (kvstart != kvend && isOpenCLSpiller()) {
            //   throw new DontBlockOnSpillDoneException();
            // }
            if (isOpenCLSpiller()) {
                throw new DontBlockOnSpillDoneException();
            }
            try {
              if (OpenCLDriver.logger != null) {
                  // LOG:PROFILE
                  // OpenCLDriver.logger.log("Blocking in collect", "mapper");
              }
              while (kvnext == kvstart) {
              // while (kvstart != kvend) {
                reporter.progress();
                spillDone.await();
              }
              if (OpenCLDriver.logger != null) {
                  // LOG:PROFILE
                  // OpenCLDriver.logger.log("Unblocking in collect", "mapper");
              }
            } catch (InterruptedException e) {
              throw (IOException)new IOException(
                  "Collector interrupted while waiting for the writer"
                  ).initCause(e);
            }
          }
        } while (kvfull);
      } finally {
        spillLock.unlock();
      }

      try {
        // serialize key bytes into buffer
        int keystart = bufindex;
        keySerializer.serialize(key);
        if (bufindex < keystart) {
          // wrapped the key; reset required
          bb.reset();
          keystart = 0;
        }
        // serialize value bytes into buffer
        final int valstart = bufindex;
        valSerializer.serialize(value);
        int valend = bb.markRecord();

        if (partition < 0 || partition >= partitions) {
          throw new IOException("Illegal partition for " + key.toString() + " (" +
              partition + ")");
        }

        mapOutputRecordCounter.increment(1);
        mapOutputByteCounter.increment(valend >= keystart
            ? valend - keystart
            : (bufvoid - keystart) + valend);

        // update accounting info
        int ind = kvindex * ACCTSIZE;
        kvoffsets[kvindex] = ind;
        kvindices[ind + PARTITION] = partition;
        kvindices[ind + KEYSTART] = keystart;
        kvindices[ind + VALSTART] = valstart;
        kvindex = kvnext;
      } catch (MapBufferTooSmallException e) {
        LOG.info("Record too large for in-memory buffer: " + e.getMessage());
        spillSingleRecord(key, value, partition);
        mapOutputRecordCounter.increment(1);
        return;
      }

    }

    /**
     * Compare logical range, st i, j MOD offset capacity.
     * Compare by partition, then by key.
     * @see IndexedSortable#compare
     */
    public int compare(int i, int j) {
      final int ii = kvoffsets[i % kvoffsets.length];
      final int ij = kvoffsets[j % kvoffsets.length];
      // sort by partition
      if (kvindices[ii + PARTITION] != kvindices[ij + PARTITION]) {
        return kvindices[ii + PARTITION] - kvindices[ij + PARTITION];
      }
      // sort by key
      return comparator.compare(kvbuffer,
          kvindices[ii + KEYSTART],
          kvindices[ii + VALSTART] - kvindices[ii + KEYSTART],
          kvbuffer,
          kvindices[ij + KEYSTART],
          kvindices[ij + VALSTART] - kvindices[ij + KEYSTART]);
    }

    /**
     * Swap logical indices st i, j MOD offset capacity.
     * @see IndexedSortable#swap
     */
    public void swap(int i, int j) {
      i %= kvoffsets.length;
      j %= kvoffsets.length;
      int tmp = kvoffsets[i];
      kvoffsets[i] = kvoffsets[j];
      kvoffsets[j] = tmp;
    }

    /**
     * Inner class managing the spill of serialized records to disk.
     */
    protected class BlockingBuffer extends DataOutputStream {

      public BlockingBuffer() {
        this(new Buffer());
      }

      private BlockingBuffer(OutputStream out) {
        super(out);
      }

      /**
       * Mark end of record. Note that this is required if the buffer is to
       * cut the spill in the proper place.
       */
      public int markRecord() {
        bufmark = bufindex;
        return bufindex;
      }

      /**
       * Set position from last mark to end of writable buffer, then rewrite
       * the data between last mark and kvindex.
       * This handles a special case where the key wraps around the buffer.
       * If the key is to be passed to a RawComparator, then it must be
       * contiguous in the buffer. This recopies the data in the buffer back
       * into itself, but starting at the beginning of the buffer. Note that
       * reset() should <b>only</b> be called immediately after detecting
       * this condition. To call it at any other time is undefined and would
       * likely result in data loss or corruption.
       * @see #markRecord()
       */
      protected synchronized void reset() throws IOException {
        // spillLock unnecessary; If spill wraps, then
        // bufindex < bufstart < bufend so contention is impossible
        // a stale value for bufstart does not affect correctness, since
        // we can only get false negatives that force the more
        // conservative path
        int headbytelen = bufvoid - bufmark;
        bufvoid = bufmark;
        if (bufindex + headbytelen < bufstart) {
          System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
          System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
          bufindex += headbytelen;
        } else {
          byte[] keytmp = new byte[bufindex];
          System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
          bufindex = 0;
          out.write(kvbuffer, bufmark, headbytelen);
          out.write(keytmp);
        }
      }
    }

    public class Buffer extends OutputStream {
      private final byte[] scratch = new byte[1];

      @Override
      public synchronized void write(int v)
          throws IOException {
        scratch[0] = (byte)v;
        write(scratch, 0, 1);
      }

      /**
       * Attempt to write a sequence of bytes to the collection buffer.
       * This method will block if the spill thread is running and it
       * cannot write.
       * @throws MapBufferTooSmallException if record is too large to
       *    deserialize into the collection buffer.
       */
      @Override
      public synchronized void write(byte b[], int off, int len)
          throws IOException {
        boolean buffull = false;
        boolean wrap = false;
        boolean dontBlockException = false;

        spillLock.lock();

        try {
          do {
            if (sortSpillException != null) {
              throw (IOException)new IOException("Spill failed"
                  ).initCause(sortSpillException);
            }

            // wrap = whether there would be enough space with a wrap to write the whole thing?
            // sufficient buffer space?
            if (bufstart <= bufend && bufend <= bufindex) {
              buffull = bufindex + len > bufvoid;
              wrap = (bufvoid - bufindex) + bufstart > len;
            } else {
              // bufindex <= bufstart <= bufend
              // bufend <= bufindex <= bufstart
              wrap = false;
              buffull = bufindex + len >= bufstart;
            }

            if (!spillRunning) {
            // if (kvstart == kvend) {
              if (kvend != kvindex) {
                // we have records we can spill
                final boolean bufsoftlimit = (bufindex > bufend)
                  ? bufindex - bufend > softBufferLimit
                  : bufend - bufindex < bufvoid - softBufferLimit;
                if (bufsoftlimit || (buffull && !wrap)) {
                  LOG.info("Spilling map output: buffer full= " + bufsoftlimit);
                  startSpill();
                }
              } else if (buffull && !wrap) {
                // We have no buffered records, and this record is too large
                // to write into kvbuffer. We must spill it directly from
                // collect
                final int size = ((bufend <= bufindex)
                  ? bufindex - bufend
                  : (bufvoid - bufend) + bufindex) + len;
                bufstart = bufend = bufindex = bufmark = 0;
                kvstart = kvend = kvindex = 0;
                bufvoid = kvbuffer.length;
                throw new MapBufferTooSmallException(size + " bytes");
              }
            }

            if (buffull && !wrap) {
              if (isOpenCLSpiller()) {
                throw new DontBlockOnSpillDoneException();
              }
              // if (kvstart != kvend && isOpenCLSpiller()) {
              //   throw new DontBlockOnSpillDoneException();
              // }

              try {
                if (OpenCLDriver.logger != null) {
                    // LOG:PROFILE
                    // OpenCLDriver.logger.log("Blocking in write", "mapper");
                }
                while (kvstart != kvend) {
                  reporter.progress();
                  spillDone.await();
                }
                if (OpenCLDriver.logger != null) {
                    // LOG:PROFILE
                    // OpenCLDriver.logger.log("Unblocking in write", "mapper");
                }
              } catch (InterruptedException e) {
                  throw (IOException)new IOException(
                      "Buffer interrupted while waiting for the writer"
                      ).initCause(e);
              }
            }
          } while (buffull && !wrap);
        } finally {
            spillLock.unlock();
        }
        // here, we know that we have sufficient space to write
        if (buffull) {
          final int gaplen = bufvoid - bufindex;
          System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
          len -= gaplen;
          off += gaplen;
          bufindex = 0;
        }
        System.arraycopy(b, off, kvbuffer, bufindex, len);

        bufindex += len;
      }
    }

    public synchronized void flush() throws IOException, ClassNotFoundException,
                                            InterruptedException {
      LOG.info("Starting flush of map output");
      spillLock.lock();
      try {
        if (OpenCLDriver.logger != null) {
            // LOG:PROFILE
            // OpenCLDriver.logger.log("Blocking in flush", "mapper");
        }
        while (kvstart != kvend) {
          reporter.progress();
          spillDone.await();
        }
        if (OpenCLDriver.logger != null) {
            // LOG:PROFILE
            // OpenCLDriver.logger.log("Unblocking in flush", "mapper");
        }
        if (sortSpillException != null) {
          throw (IOException)new IOException("Spill failed"
              ).initCause(sortSpillException);
        }
        noMoreSpilling = true;

        if (kvend != kvindex) {
          kvend = kvindex;
          bufend = bufmark;
          sortAndSpill(flushCombineCollector, kvstart, kvend,
              bufstart, bufend, bufvoid);
        }
        spillReady.signal();
      } catch (InterruptedException e) {
        throw (IOException)new IOException(
            "Buffer interrupted while waiting for the writer"
            ).initCause(e);
      } finally {
        spillLock.unlock();
      }
      assert !spillLock.isHeldByCurrentThread();
      // shut down spill thread and wait for it to exit. Since the preceding
      // ensures that it is finished with its work (and sortAndSpill did not
      // throw), we elect to use an interrupt instead of setting a flag.
      // Spilling simultaneously from this thread while the spill thread
      // finishes its work might be both a useful way to extend this and also
      // sufficient motivation for the latter approach.
      try {
        // spillThread.interrupt();
        spillThread.join();
      } catch (InterruptedException e) {
        throw (IOException)new IOException("Spill failed"
            ).initCause(e);
      }
      // release sort buffer before the merge
      kvbuffer = null;
      mergeParts();
      Path outputPath = mapOutputFile.getOutputFile();
      fileOutputByteCounter.increment(rfs.getFileStatus(outputPath).getLen());
    }

    public void close() { }

    private int diffWithWrap(int bottom, int top, int limit) {
        if (top > bottom) {
            return top - bottom;
        } else {
            return (limit - bottom) + top;
        }
    }

    public void releaseCurrentlySpilling(boolean callingFromSpillThread) {

        if (!callingFromSpillThread) {
            spillLock.lock();
        }

        if (alreadyReleased) {
          // Don't unlock, as we are being called from the SpillThread
          if (!callingFromSpillThread) spillLock.unlock();
          return;
        }
        alreadyReleased = true;

        if (bufend < bufindex && bufindex < bufstart) {
          bufvoid = kvbuffer.length;
        }
        kvstart = kvend;
        bufstart = bufend;

        synchronized(BufferRunner.somethingHappened) {
            BufferRunner.somethingHappened.set(true);
            BufferRunner.somethingHappened.notify();
        }
        if (!callingFromSpillThread) spillLock.unlock();
    }

    protected class SpillThread extends Thread {

      @Override
      public void run() {
        spillLock.lock();

        spillThreadRunning = true;
        try {
          synchronized(BufferRunner.somethingHappened) {
              BufferRunner.somethingHappened.set(true);
              BufferRunner.somethingHappened.notify();
          }
          while (!noMoreSpilling) {

            spillDone.signalAll();

            while (kvstart == kvend && !noMoreSpilling) {
              spillReady.await();
            }

            if (noMoreSpilling) break;

            try {
              if (!alreadyReleased) {
                  throw new RuntimeException(
                      "Seems like we missed the last release?");
              }
              alreadyReleased = false;

              final int local_kvstart = kvstart;
              final int local_kvend = kvend;
              final int local_bufstart = bufstart;
              final int local_bufend = bufend;
              final int local_bufvoid = bufvoid;

              spillRunning = true;
              spillLock.unlock();

              sortAndSpill(combineCollector, local_kvstart, local_kvend,
                  local_bufstart, local_bufend, local_bufvoid);
            } catch (Exception e) {
              sortSpillException = e;
            } catch (Throwable t) {
              sortSpillException = t;
              String logMsg = "Task " + getTaskID() + " failed : " 
                              + StringUtils.stringifyException(t);
              reportFatalError(getTaskID(), t, logMsg);
            } finally {
                spillLock.lock();
                spillRunning = false;
                releaseCurrentlySpilling(true);
                final int newDiff;
                if (kvstart == kvindex) {
                    newDiff = 0;
                } else {
                    newDiff = diffWithWrap(kvstart, kvindex, kvoffsets.length);
                }
                double newKvRatio = (double)newDiff / (double)kvoffsets.length;
                double newBufRatio = (double)diffWithWrap(bufstart, bufmark, bufvoid) / (double)bufvoid;
                if (newKvRatio > quickRestartPercent) {
                  LOG.info("Immediate relaunch, kv-ratio="+newKvRatio+" buf-ratio="+newBufRatio);

                  if (newKvRatio < maxRestartPercent) {
                      kvend = kvindex;
                      bufend = bufmark;
                  } else {
                      kvend = (kvstart + (int)(kvoffsets.length * maxRestartPercent)) % kvoffsets.length;
                      bufend = kvindices[kvoffsets[kvend] + KEYSTART];
                  }
                  LOG.info("bufstart = " + numFormat.format(bufstart) + "; bufend = " + numFormat.format(bufend) +
                      "; bufvoid = " + numFormat.format(bufvoid));
                  LOG.info("kvstart = " + numFormat.format(kvstart) + "; kvend = " + numFormat.format(kvend) +
                      "; length = " + numFormat.format(kvoffsets.length));
                }
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          spillLock.unlock();
          spillThreadRunning = false;
        }
      }
    }

    private synchronized void startSpill() {
      LOG.info("bufstart = " + numFormat.format(bufstart) + "; bufend = " + numFormat.format(bufmark) +
               "; bufvoid = " + numFormat.format(bufvoid));
      LOG.info("kvstart = " + numFormat.format(kvstart) + "; kvend = " + numFormat.format(kvindex) +
               "; length = " + numFormat.format(kvoffsets.length));
      softBufferLimit = (int)(kvbuffer.length * spillper);
      softRecordLimit = (int)(kvoffsets.length * spillper);
      kvend = kvindex;
      bufend = bufmark;
      spillReady.signal();
    }

    class NextedWrapper implements IterateAndPartition {
        private boolean nexted = true;
        private final IterateAndPartition iter;

        public NextedWrapper(IterateAndPartition iter) {
            this.iter = iter;
        }
        @Override
        public final DataInputBuffer getKey() throws IOException {
            return iter.getKey();
        }
        @Override
        public final DataInputBuffer getValue() throws IOException {
            return iter.getValue();
        }
        @Override
        public final boolean next() throws IOException {
            final boolean result;
            if (!nexted) {
                result = iter.next();
            } else {
                nexted = false;
                result = true;
            }
            return result;
        }
        @Override
        public final void close() throws IOException {
            iter.close();
        }
        @Override
        public final Progress getProgress() {
            return iter.getProgress();
        }
        @Override
        public final boolean supportsBulkReads() {
            return iter.supportsBulkReads();
        }
        @Override
        public final HadoopCLDataInput getBulkReader() {
            return iter.getBulkReader();
        }
        @Override
        public final int getPartitionOfCurrent(int partitions) {
            return iter.getPartitionOfCurrent(partitions);
        }
    }

    class MergedIterator implements IterateAndPartition {
        private IteratorWrapper iters = null;
        private int nIters;
        private final RawComparator writeableComp;
        private final Comparator<IteratorWrapper> comp = new Comparator<IteratorWrapper>() {
            @Override
            public int compare(IteratorWrapper iter1, IteratorWrapper iter2) {
                final DataInputBuffer key1;
                final DataInputBuffer key2;
                try {
                    key1 = iter1.iter.getKey();
                    key2 = iter2.iter.getKey();
                } catch (IOException io) {
                    throw new RuntimeException(io);
                }
                return writeableComp.compare(key1.getData(), key1.getPosition(), key1.getLength(),
                    key2.getData(), key2.getPosition(), key2.getLength());
            }
            @Override
            public boolean equals(Object o) {
                throw new UnsupportedOperationException();
            }
        };

        public IterateAndPartition completePrep() {
            if (iters == null) {
                throw new RuntimeException("Unexpected empty iters list");
            }

            if (nIters > 1) {
                return this;
            } else {
                return new NextedWrapper(iters.iter);
            }
        }

        public MergedIterator(RawComparator writeableComp) {
            this.writeableComp = writeableComp;
        }

        public int size() {
            return nIters;
        }

        private void sortedInsertHelper(IteratorWrapper iter, IteratorWrapper start) {
            IteratorWrapper curr = start;
            IteratorWrapper prev = null;
            while (curr != null && comp.compare(curr, iter) < 0) {
                prev = curr;
                curr = curr.next;
            }

            if (prev == null) {
                curr.prev = iter;
                iter.next = curr;
                iter.prev = null;
                this.iters = iter;
            } else if (curr == null) {
                prev.next = iter;
                iter.next = null;
                iter.prev = prev;
            } else {
                iter.next = curr;
                iter.prev = prev;
                prev.next = iter;
                curr.prev = iter;
            }

        }

        /* For items not in the list yet */
        private void sortedInsert(IteratorWrapper iter) {
            if (this.iters == null) {
                this.iters = iter;
                iter.next = null;
                iter.prev = null;
            } else {
                sortedInsertHelper(iter, this.iters);
            }
        }

        /* For items in the list already */
        private void sortedUpdate() {
            if (this.iters.next == null) {
                return;
            }

            IteratorWrapper curr = this.iters;
            this.iters = this.iters.next;
            this.iters.prev = null;
            curr.next = curr.prev = null;

            sortedInsertHelper(curr, this.iters);
        }

        public void addIter(IterateAndPartition iter) {
            sortedInsert(new IteratorWrapper(iter));
            nIters++;
        }
        @Override
        public int getPartitionOfCurrent(int partitions) {
            return iters.iter.getPartitionOfCurrent(partitions);
        }
        @Override
        public DataInputBuffer getKey() throws IOException {
            return iters.iter.getKey();
        }
        @Override
        public DataInputBuffer getValue() throws IOException {
            return iters.iter.getValue();
        }
        @Override
        public boolean next() throws IOException {
            if (iters == null) return false;

            final boolean more;
            if (iters.alreadyNexted) {
                iters.alreadyNexted = false;
                more = true;
            } else {
                more = iters.iter.next();
            }

            if (!more) {
                this.iters = this.iters.next;
                if (this.iters != null) this.iters.prev = null;
            } else {
                sortedUpdate();
            }

            if (this.iters == null) {
                return false;
            } else {
                return true;
            }
        }

        @Override
        public void close() throws IOException { }

        @Override
        public Progress getProgress() { return null; }

        @Override
        public boolean supportsBulkReads() {
            boolean allSupport = true;
            IteratorWrapper curr = iters;
            while (curr != null) {
                if (!curr.iter.supportsBulkReads()) {
                    allSupport = false;
                    break;
                }
                curr = curr.next;
            }
            return allSupport;
        }

        @Override
        public HadoopCLDataInput getBulkReader() {
            final List<ReaderWrapper> readers =
                new LinkedList<ReaderWrapper>();
            final Comparator<ReaderWrapper> comp = new Comparator<ReaderWrapper>() {
                @Override
                public int compare(ReaderWrapper iter1, ReaderWrapper iter2) {
                    final int result;
                    try {
                        result = iter1.reader.compareKeys(iter2.reader);
                    } catch (IOException io) {
                        throw new RuntimeException(io);
                    }
                    iter1.reader.reset();
                    iter2.reader.reset();
                    return result;
                }
                @Override
                public boolean equals(Object o) {
                    throw new UnsupportedOperationException();
                }
            };

            try {
                IteratorWrapper iw = iters;
                while (iw != null) {
                    HadoopCLDataInput tmp = iw.iter.getBulkReader();
                    if (tmp.hasMore()) {
                        tmp.nextKey();
                        readers.add(new ReaderWrapper(tmp));
                    }
                    iw = iw.next;
                }
            } catch (IOException io) {
                throw new RuntimeException(io);
            }

            Collections.sort(readers, comp);

            return new HadoopCLBulkMapperReader() {
                boolean first = true;
                private ReaderWrapper least = readers.get(0);

                @Override
                public final boolean hasMore() throws IOException {
                    if (least == null) {
                        return false;
                    }

                    if (!first) {
                        if (!least.reader.hasMore()) {
                            readers.remove(least);
                        } else {
                            try {
                                least.reader.nextKey();
                            } catch (IOException io) {
                                throw new RuntimeException(io);
                            }
                            Collections.sort(readers, comp);
                        }
                        if (readers.isEmpty()) {
                            least = null;
                        } else {
                            least = readers.get(0);
                        }
                    }

                    first = false;
                    return least != null;
                }
                @Override
                public int compareKeys(HadoopCLDataInput other) throws IOException {
                    return this.least.reader.compareKeys(other);
                }
                @Override
                public final void nextKey() throws IOException {
                    // if (this.least.alreadyNexted) {
                    //     this.least.alreadyNexted = false;
                    // } else {
                    //     final boolean more = this.least.reader.hasMore();
                    //     if (!more) {
                    //         readers.remove(this.least);
                    //     } else {
                    //         this.least.reader.nextKey();
                    //         Collections.sort(readers, comp);
                    //     }
                    // }
                    // previousLeast = least;
                    // if (readers.isEmpty()) {
                    //     this.least = null;
                    // } else {
                    //     this.least = readers.get(0);
                    // }
                }
                @Override
                public final void nextValue() throws IOException {
                    this.least.reader.nextValue();
                }
                @Override
                public final void prev() {
                    this.least.reader.reset();
                }
                @Override
                public final void readFully(int[] b, int off, int len) throws IOException {
                    this.least.reader.readFully(b, off, len);
                }
                @Override
                public final void readFully(double[] b, int off, int len) throws IOException {
                    this.least.reader.readFully(b, off, len);
                }
                @Override
                public final int readInt() throws IOException {
                    return this.least.reader.readInt();
                }

            };
        }

        class ReaderWrapper {
            public final HadoopCLDataInput reader;
            public boolean alreadyNexted;

            public ReaderWrapper(HadoopCLDataInput reader) {
                this.reader = reader;
                this.alreadyNexted = true;
            }
        }

        class IteratorWrapper {
            public final IterateAndPartition iter;
            public boolean alreadyNexted;
            public IteratorWrapper prev;
            public IteratorWrapper next;

            public IteratorWrapper(IterateAndPartition iter) {
                this.iter = iter;
                this.alreadyNexted = true;
                this.prev = null;
                this.next = null;
            }
        }

        @Override
        public String toString() {
            return this.getClass().getName()+",size="+this.size();
        }
    }

    private void sortAndSpill(final CombineOutputCollector<K, V> combineCollector,
          final int kvstart, final int kvend,
          final int bufstart, final int bufend, final int bufvoid)
          throws IOException, ClassNotFoundException, InterruptedException {
      //approximate the length of the output file to be the length of the
      //buffer + header lengths for the partitions
      LOG.info("Spilling! kvstart=" + numFormat.format(kvstart) + " kvend=" + numFormat.format(kvend));
      long size = (bufend >= bufstart
          ? bufend - bufstart
          : (bufvoid - bufend) + bufstart) +
                  partitions * APPROX_HEADER_LENGTH;

      final int spillNo = numSpills.getAndIncrement();
      // create spill file
      final SpillRecord spillRec = new SpillRecord(partitions, spillNo);
      // final int spillNo = numSpills++;
      final Path filename =
          mapOutputFile.getSpillFileForWrite(spillNo, size);

      final int endPosition = (kvend > kvstart)
        ? kvend
        : kvoffsets.length + kvend;
      sorter.sort(MapOutputBuffer.this, kvstart, endPosition, reporter);

      final IterateAndPartition iter;
      if (combinerRunner == null) {
          final int tmp_kvstart = kvstart;
          iter = new IterateAndPartition() {
              private int pointer = tmp_kvstart - 1;
              private final int end = endPosition;
              private final DataInputBuffer key = new DataInputBuffer();
              private final InMemValBytes value = new InMemValBytes();
              public DataInputBuffer getKey() throws IOException {
                  final int kvoff = kvoffsets[pointer % kvoffsets.length];
                  key.reset(kvbuffer, kvindices[kvoff + KEYSTART],
                            (kvindices[kvoff + VALSTART] - 
                             kvindices[kvoff + KEYSTART]));
                  return key;
              }
              public DataInputBuffer getValue() throws IOException {
                  final int kvoff = kvoffsets[pointer % kvoffsets.length];
                  getVBytesForOffset(kvoff, value);
                  return value;
              }
              public int getPartitionOfCurrent(int partitions) {
                  final int kvoff = kvoffsets[pointer % kvoffsets.length];
                  return kvindices[kvoff + PARTITION];
              }
              public boolean next() throws IOException {
                  pointer++;
                  return pointer < endPosition;
              }
              public void close() throws IOException {
              }
              public Progress getProgress() { return null; }
              public boolean supportsBulkReads() {
                  return false;
              }
              public HadoopCLDataInput getBulkReader() {
                  throw new UnsupportedOperationException();
              }
          };
      } else {
          iter = new MRResultIterator(kvstart, endPosition);
      }

      FSDataOutputStream out = null;
      try {
        out = rfs.create(filename);

        if (combinerRunner == null) {
            BulkWriter<K, V> writer = new BulkWriter<K, V>(job, out,
                keyClass, valClass, codec, spilledRecordsCounter, iter);
            while (iter.next()) {
                writer.append(iter.getKey(), iter.getValue());
            }
            writer.close();

            HashMap<Integer, Long> partitionSegmentStarts =
                writer.getPartitionSegmentStarts();
            HashMap<Integer, Long> partitionRawLengths =
                writer.getPartitionRawLengths();
            HashMap<Integer, Long> partitionCompressedLengths =
                writer.getPartitionCompressedLengths();

            for (int part = 0; part < partitions; part++) {
                final IndexRecord rec = new IndexRecord();

                rec.startOffset = partitionSegmentStarts.get(part);
                rec.rawLength = partitionRawLengths.get(part);
                rec.partLength = partitionCompressedLengths.get(part);
                spillRec.putIndex(rec, part);
            }

            synchronized (MapTask.indexCacheList) {
                MapTask.indexCacheList.put(spillRec.getSpillNo(), spillRec);
                MapTask.totalIndexCacheMemory +=
                    spillRec.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
            }

        } else {
            SortedWriter<K, V> writer = new SortedWriter<K, V>(job, out,
                keyClass, valClass, codec, spilledRecordsCounter, comparator,
                true, null);
            combineCollector.setWriter(writer);
            combinerRunner.setDirectCombiner(true);
            combinerRunner.setValidToRelease(true);
            combinerRunner.combine(iter, combineCollector);

            writer.close();

            HashMap<Integer, Long> partitionSegmentStarts =
                writer.getPartitionSegmentStarts();
            HashMap<Integer, Long> partitionRawLengths =
                writer.getPartitionRawLengths();
            HashMap<Integer, Long> partitionCompressedLengths =
                writer.getPartitionCompressedLengths();

            for (int part = 0; part < partitions; part++) {
                final IndexRecord rec = new IndexRecord();

                rec.startOffset = partitionSegmentStarts.get(part);
                rec.rawLength = partitionRawLengths.get(part);
                rec.partLength = partitionCompressedLengths.get(part);
                spillRec.putIndex(rec, part);
            }

            synchronized (MapTask.indexCacheList) {
                MapTask.indexCacheList.put(spillRec.getSpillNo(), spillRec);
                MapTask.totalIndexCacheMemory +=
                    spillRec.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
            }
        }

        if (MapTask.totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(spillNo, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRec.writeToFile(indexFilename, job);
        } else {
          synchronized (MapTask.indexCacheList) {
              MapTask.indexCacheList.put(spillRec.getSpillNo(), spillRec);
              MapTask.totalIndexCacheMemory +=
                spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
          }
        }
        LOG.info("Finished spill " + spillNo);
      } finally {
        if (out != null) out.close();
      }
    }

    /**
     * Handles the degenerate case where serialization fails to fit in
     * the in-memory buffer, so we must spill the record from collect
     * directly to a spill file. Consider this "losing".
     */
    private void spillSingleRecord(final K key, final V value,
                                   int partition) throws IOException {
      long size = kvbuffer.length + partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      try {
        // create spill file
        int spillNo = numSpills.getAndIncrement();
        final SpillRecord spillRec = new SpillRecord(partitions, spillNo);
        final Path filename =
            mapOutputFile.getSpillFileForWrite(spillNo, size);
        out = rfs.create(filename);
        
        // we don't run the combiner for a single record
        IndexRecord rec = new IndexRecord();
        for (int i = 0; i < partitions; ++i) {
          IFile.Writer<K, V> writer = null;
          try {
            long segmentStart = out.getPos();
            // Create a new codec, don't care!
            writer = new IFile.Writer<K,V>(job, out, keyClass, valClass, codec,
                                            spilledRecordsCounter);

            if (i == partition) {
              final long recordStart = out.getPos();
              writer.append(key, value);
              // Note that our map byte count will not be accurate with
              // compression
              mapOutputByteCounter.increment(out.getPos() - recordStart);
            }
            writer.close();

            // record offsets
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength();
            rec.partLength = writer.getCompressedLength();
            spillRec.putIndex(rec, i);

            writer = null;
          } catch (IOException e) {
            if (null != writer) writer.close();
            throw e;
          }
        }
        if (MapTask.totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(spillNo, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRec.writeToFile(indexFilename, job);
        } else {
          synchronized (MapTask.indexCacheList) {
              MapTask.indexCacheList.put(spillRec.getSpillNo(), spillRec);
              MapTask.totalIndexCacheMemory +=
                spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
          }
        }
        // ++numSpills;
      } finally {
        if (out != null) out.close();
      }
    }

    /**
     * Given an offset, populate vbytes with the associated set of
     * deserialized value bytes. Should only be called during a spill.
     */
    private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
      final int nextindex = (kvoff / ACCTSIZE ==
                            (kvend - 1 + kvoffsets.length) % kvoffsets.length)
        ? bufend
        : kvindices[(kvoff + ACCTSIZE + KEYSTART) % kvindices.length];
      int vallen = (nextindex >= kvindices[kvoff + VALSTART])
        ? nextindex - kvindices[kvoff + VALSTART]
        : (bufvoid - kvindices[kvoff + VALSTART]) + nextindex;
      vbytes.reset(kvbuffer, kvindices[kvoff + VALSTART], vallen);
    }

    /**
     * Inner class wrapping valuebytes, used for appendRaw.
     */
    protected class InMemValBytes extends DataInputBuffer {
      private byte[] buffer;
      private int start;
      private int length;
            
      public void reset(byte[] buffer, int start, int length) {
        this.buffer = buffer;
        this.start = start;
        this.length = length;
        
        if (start + length > bufvoid) {
          this.buffer = new byte[this.length];
          final int taillen = bufvoid - start;
          System.arraycopy(buffer, start, this.buffer, 0, taillen);
          System.arraycopy(buffer, 0, this.buffer, taillen, length-taillen);
          this.start = 0;
        }
        
        super.reset(this.buffer, this.start, this.length);
      }
    }

    protected class MRResultIterator implements IterateAndPartition {
      private final DataInputBuffer keybuf = new DataInputBuffer();
      private final InMemValBytes vbytes = new InMemValBytes();
      private final int start;
      private final int end;
      private int current;
      public MRResultIterator(int start, int end) {
        this.start = start;
        this.end = end;
        current = start - 1;
      }
      public boolean next() throws IOException {
        return ++current < end;
      }
      public DataInputBuffer getKey() throws IOException {
        final int kvoff = kvoffsets[current % kvoffsets.length];
        keybuf.reset(kvbuffer, kvindices[kvoff + KEYSTART],
                     kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART]);
        return keybuf;
      }
      public DataInputBuffer getValue() throws IOException {
        getVBytesForOffset(kvoffsets[current % kvoffsets.length], vbytes);
        return vbytes;
      }
      public int getPartitionOfCurrent(int partitions) {
          final int kvoff = kvoffsets[current % kvoffsets.length];
          return kvindices[kvoff + PARTITION];
      }
      public Progress getProgress() {
        return null;
      }
      public void close() { }
      public boolean supportsBulkReads() {
          return true;
      }
      public HadoopCLDataInput getBulkReader() {
          return new HadoopCLBulkCombinerReader(start, end, kvoffsets,
              kvindices, kvbuffer, bufvoid);
      }
    }

    private void mergeParts() throws IOException, InterruptedException, 
                                     ClassNotFoundException {
      // get the approximate size of the final output/index files
      long finalOutFileSize = 0;
      long finalIndexFileSize = 0;
      final int totalSpills = numSpills.get();
      final Path[] filename = new Path[totalSpills];
      final TaskAttemptID mapId = getTaskID();

      for(int i = 0; i < totalSpills; i++) {
        filename[i] = mapOutputFile.getSpillFile(i);
        finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
      }
      if (totalSpills == 1) { //the spill is the final output
        rfs.rename(filename[0],
            new Path(filename[0].getParent(), "file.out"));
        if (indexCacheList.size() == 0) {
          rfs.rename(mapOutputFile.getSpillIndexFile(0),
              new Path(filename[0].getParent(),"file.out.index"));
        } else {
          indexCacheList.get(0).writeToFile(
                new Path(filename[0].getParent(),"file.out.index"), job);
        }
        return;
      }

      // read in paged indices
      for (int i = indexCacheList.size(); i < totalSpills; ++i) {
        Path indexFileName = mapOutputFile.getSpillIndexFile(i);
        SpillRecord record = new SpillRecord(indexFileName, job, null);
        indexCacheList.put(record.getSpillNo(), record);
      }

      //make correction in the length to include the sequence file header
      //lengths for each partition
      finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
      finalIndexFileSize = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      Path finalOutputFile =
          mapOutputFile.getOutputFileForWrite(finalOutFileSize);
      Path finalIndexFile =
          mapOutputFile.getOutputIndexFileForWrite(finalIndexFileSize);

      //The output stream for the final single output file
      FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

      if (totalSpills == 0) {
        //create dummy files
        IndexRecord rec = new IndexRecord();
        SpillRecord sr = new SpillRecord(partitions, 0);
        try {
          for (int i = 0; i < partitions; i++) {
            long segmentStart = finalOut.getPos();
            Writer<K, V> writer =
              new Writer<K, V>(job, finalOut, keyClass, valClass, codec, null);
            writer.close();
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength();
            rec.partLength = writer.getCompressedLength();
            sr.putIndex(rec, i);
          }
          sr.writeToFile(finalIndexFile, job);
        } finally {
          finalOut.close();
        }
        return;
      }
      {
        IndexRecord rec = new IndexRecord();
        final SpillRecord spillRec = new SpillRecord(partitions, 0);
        for (int parts = 0; parts < partitions; parts++) {
          final int tmp_parts = parts;
          //create the segments to be merged
          List<Segment<K,V>> segmentList =
            new ArrayList<Segment<K, V>>(totalSpills);
          for(int i = 0; i < totalSpills; i++) {
            IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

            Segment<K,V> s =
              new Segment<K,V>(job, rfs, filename[i], indexRecord.startOffset,
                               indexRecord.partLength, codec, true);
            segmentList.add(i, s);

            if (LOG.isDebugEnabled()) {
              LOG.debug("MapId=" + mapId + " Reducer=" + parts +
                  "Spill =" + i + "(" + indexRecord.startOffset + "," +
                  indexRecord.rawLength + ", " + indexRecord.partLength + ")");
            }
          }

          //merge
          @SuppressWarnings("unchecked")
          RawKeyValueIterator kvIter = Merger.merge(job, rfs,
                         keyClass, valClass, codec,
                         segmentList, job.getInt("io.sort.factor", 100),
                         new Path(mapId.toString()),
                         job.getOutputKeyComparator(), reporter,
                         null, spilledRecordsCounter);

          //write merged output to disk
          long segmentStart = finalOut.getPos();
          final Writer<K, V> writer;
          if (combinerRunner == null || totalSpills < minSpillsForCombine) {
            writer =
                new Writer<K, V>(job, finalOut, keyClass, valClass, codec,
                                 spilledRecordsCounter);
            Merger.writeFile(kvIter, writer, reporter, job);
          } else {
            writer = new SortedWriter(job, finalOut, keyClass, valClass, codec,
                spilledRecordsCounter, comparator, false,
                new HadoopCLPartitioner() {
                    @Override
                    public int getPartitionOfCurrent(int partitions) {
                        return tmp_parts;
                    }
                });
            combineCollector.setWriter(writer);
            combinerRunner.setDirectCombiner(false);
            combinerRunner.combine(kvIter, combineCollector);
          }

          //close
          writer.close();

          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          spillRec.putIndex(rec, parts);
        }
        spillRec.writeToFile(finalIndexFile, job);
        finalOut.close();
        for(int i = 0; i < totalSpills; i++) {
          rfs.delete(filename[i],true);
        }
      }
    }

  } // MapOutputBuffer
  
  /**
   * Exception indicating that the allocated sort buffer is insufficient
   * to hold the current record.
   */
  @SuppressWarnings("serial")
  private static class MapBufferTooSmallException extends IOException {
    public MapBufferTooSmallException(String s) {
      super(s);
    }
  }

}
