package org.apache.hadoop.mapred;

import java.io.IOException;
import org.apache.hadoop.io.DataInputBuffer;

public interface MergeReader<K,V> {
  public long getLength();
  public long getPosition() throws IOException;
  public boolean next(DataInputBuffer key,
      DataInputBuffer val) throws IOException;
  public void close() throws IOException;
}
