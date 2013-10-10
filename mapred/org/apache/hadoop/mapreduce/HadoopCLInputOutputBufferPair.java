package org.apache.hadoop.mapreduce;

public class HadoopCLInputOutputBufferPair {
    private final HadoopCLInputBuffer inputBuffer;
    private final HadoopCLOutputBuffer outputBuffer;

    public HadoopCLInputOutputBufferPair(HadoopCLInputBuffer inputBuffer,
            HadoopCLOutputBuffer outputBuffer) {
        this.inputBuffer = inputBuffer;
        this.outputBuffer = outputBuffer;
    }

    public HadoopCLInputBuffer inputBuffer() { return this.inputBuffer; }
    public HadoopCLOutputBuffer outputBuffer() { return this.outputBuffer; }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HadoopCLInputOutputBufferPair) {
            HadoopCLInputOutputBufferPair other = (HadoopCLInputOutputBufferPair)obj;
            return this.inputBuffer.id == other.inputBuffer.id &&
                this.outputBuffer.id == other.outputBuffer.id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.inputBuffer.id + this.outputBuffer.id;
    }
}
