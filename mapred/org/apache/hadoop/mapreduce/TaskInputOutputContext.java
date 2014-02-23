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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.io.DataInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.io.KVCollection;

/**
 * A context object that allows input and output from the task. It is only
 * supplied to the {@link Mapper} or {@link Reducer}.
 * @param <KEYIN> the input key type for the task
 * @param <VALUEIN> the input value type for the task
 * @param <KEYOUT> the output key type for the task
 * @param <VALUEOUT> the output value type for the task
 */
public abstract class TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
       extends TaskAttemptContext implements Progressable {
  private RecordWriter<KEYOUT,VALUEOUT> output;
  private StatusReporter reporter;
  private OutputCommitter committer;
  private final ContextType ctxType;
  private final String label;

  public static enum ContextType {
      Mapper, Reducer, Combiner
  }

  public TaskInputOutputContext(Configuration conf, TaskAttemptID taskid,
                                RecordWriter<KEYOUT,VALUEOUT> output,
                                OutputCommitter committer,
                                StatusReporter reporter, ContextType setType,
                                String label) {
    super(conf, taskid);
    this.output = output;
    this.reporter = reporter;
    this.committer = committer;
    this.ctxType = setType;
    this.label = label;
  }

  public abstract boolean supportsBulkReads();
  public abstract HadoopCLDataInput getBulkReader();

  /**
   * Advance to the next key, value pair, returning null if at end.
   * @return the key object that was read into, or null if no more
   */
  public abstract boolean nextKeyValue() throws IOException, InterruptedException;
 
  /**
   * Get the current key.
   * @return the current key object or null if there isn't one
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract 
  KEYIN getCurrentKey() throws IOException, InterruptedException;

  /**
   * Get the current value.
   * @return the value object that was read into
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract VALUEIN getCurrentValue() throws IOException, 
                                                   InterruptedException;

  public ContextType getContextType() {
      return this.ctxType;
  }

  public String getLabel() {
      return this.label;
  }

  /**
   * Generate an output key/value pair.
   */
  public void write(KEYOUT key, VALUEOUT value
                    ) throws IOException, InterruptedException {
    output.write(key, value);
  }

  public int writeCollection(KVCollection<KEYOUT,VALUEOUT> coll) throws IOException, InterruptedException {
      return output.writeCollection(coll);
  }

  public void writeChunk(byte[] buffer, int length) throws IOException, InterruptedException {
      output.writeChunk(buffer, length);
  }

  public StatusReporter getReporter() {
    return reporter;
  }

  public Counter getCounter(Enum<?> counterName) {
    return reporter.getCounter(counterName);
  }

  public Counter getCounter(String groupName, String counterName) {
    return reporter.getCounter(groupName, counterName);
  }

  @Override
  public void progress() {
    reporter.progress();
  }

  @Override
  public void setStatus(String status) {
    reporter.setStatus(status);
  }
  
  public OutputCommitter getOutputCommitter() {
    return committer;
  }

  public abstract void signalDoneReading();
}
