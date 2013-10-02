package org.apache.hadoop.mapreduce;

public class BufferTypeContainer<BufferType extends HadoopCLBuffer> {
    private final BufferType buffer;
    public BufferTypeContainer(BufferType buffer) { this.buffer = buffer; }
    public BufferType get() { return this.buffer; }
}
