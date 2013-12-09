package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.MergeReader;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;

public class SortedReader<K, V> implements MergeReader<K, V>, IndexedSortable {
  private final IFile.Reader reader;
  private int position;
  private long bytePosition;
  private RawComparator comparator;
  private final ArrayList<BufferedElement> keys;
  private final ArrayList<BufferedElement> vals;

  public SortedReader(Configuration conf, FSDataInputStream in, long length,
      CompressionCodec codec, Counters.Counter readsCounter,
      RawComparator comparator) throws IOException {
    reader = new IFile.Reader<K, V>(conf, in, length, codec, readsCounter);
    this.comparator = comparator;
    this.position = 0;
    this.bytePosition = 0;
    this.keys = new ArrayList<BufferedElement>();
    this.vals = new ArrayList<BufferedElement>();

    final DataInputBuffer key = new DataInputBuffer();
    final DataInputBuffer val = new DataInputBuffer();
    while (reader.next(key, val)) {
      // TODO may be reading these incorrectly and need to use getPosition as well?
      keys.add(new BufferedElement(key.getData(), key.getPosition(), key.getLength() - key.getPosition()));
      vals.add(new BufferedElement(val.getData(), val.getPosition(), val.getLength() - val.getPosition()));
    }
    reader.close();

    new QuickSort().sort(this, 0, keys.size());
  }

  @Override
  public long getLength() {
    return this.reader.getLength();
  }

  @Override
  public long getPosition() throws IOException {
    return this.bytePosition;
  }

  @Override
  public boolean next(DataInputBuffer key,
      DataInputBuffer val) throws IOException {
    if (this.position >= this.keys.size()) return false;
    BufferedElement currentKey = this.keys.get(this.position);
    BufferedElement currentVal = this.vals.get(this.position);
    key.reset(currentKey.data, currentKey.length);
    val.reset(currentVal.data, currentVal.length);
    this.position++;
    this.bytePosition += (currentKey.length + currentVal.length);
    return true;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public int compare(int i, int j) {
    BufferedElement keyi = this.keys.get(i);
    BufferedElement keyj = this.keys.get(j);
    return this.comparator.compare(keyi.data, 0, keyi.length,
        keyj.data, 0, keyj.length);
  }

  @Override
  public void swap(int i, int j) {
    BufferedElement tmp = this.keys.get(i);
    this.keys.set(i, this.keys.get(j));
    this.keys.set(j, tmp);

    tmp = this.vals.get(i);
    this.vals.set(i, this.vals.get(j));
    this.vals.set(j, tmp);
  }

  class BufferedElement {
    public byte[] data;
    public int length;
    public BufferedElement(byte[] d, int p, int l) {
      this.data = new byte[l];
      System.arraycopy(d, p, this.data, 0, l);
      this.length = l;
    }
  }
}
