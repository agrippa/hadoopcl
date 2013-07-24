package org.apache.hadoop.mapreduce;

import java.util.List;

interface HadoopCLPredictor<RecordingType> {
    public double predict(RecordingType rt);
    public void recharacterize(HadoopCLRecording<RecordingType> newRecord,
            List<HadoopCLRecording<RecordingType>> allRecordings);
    public abstract List<String> serializeForOutput();
    public abstract void initializeFromTokens(List<String> tokens);
    public abstract void characterizationInitialization(
            List<HadoopCLRecording<RecordingType>> records);
}
