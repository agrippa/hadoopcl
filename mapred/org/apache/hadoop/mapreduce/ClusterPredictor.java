package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;

public class ClusterPredictor<RecordingType> implements HadoopCLPredictor<RecordingType> {
    final private List<HadoopCLCluster<RecordingType>> clusters;
    private final static double MIN_ATTRACTION = 0.005;
    private boolean modifiable;

    public ClusterPredictor() {
        this.clusters = new LinkedList<HadoopCLCluster<RecordingType>>();
        this.modifiable = true;
    }

    @Override
    public void characterizationInitialization(
            List<HadoopCLRecording<RecordingType>> records) {
        for(HadoopCLRecording<RecordingType> r : records) {
            this.recharacterize(r, null);
        }
    }

    @Override
    public void recharacterize(HadoopCLRecording<RecordingType> newRecord,
            List<HadoopCLRecording<RecordingType>> recordings) {
        if(!this.modifiable) {
            throw new RuntimeException("Recharacterizing a non-modifiable predictor (cluster)");
        }
        double maxAttraction = -1.0;
        HadoopCLCluster<RecordingType> closestCluster = null;
        for(HadoopCLCluster<RecordingType> cluster : clusters) {
            double attraction = cluster.attraction(newRecord);
            if(attraction > MIN_ATTRACTION && attraction > maxAttraction) {
                maxAttraction = attraction;
                closestCluster = cluster;
            }
        }
        //System.out.println("DIAGNOSTICS: Got attraction "+maxAttraction);
        if(closestCluster != null) {
            closestCluster.addMember(newRecord);
        } else {
            HadoopCLCluster<RecordingType> newCluster = new HadoopCLCluster<RecordingType>(newRecord);
            this.clusters.add(newCluster);
        }

        for(int i = 0; i < clusters.size(); i++) {
            for(int j = i+1; j < clusters.size(); j++) {
                clusters.get(i).redistributeMembers(clusters.get(j));
            }
        }
    }

    @Override
    public double predict(RecordingType other) {
        double maxAttraction = -1.0;
        HadoopCLCluster<RecordingType> closestCluster = null;
        for(HadoopCLCluster<RecordingType> cluster : clusters) {
            double attraction = cluster.attraction(other);
            if(attraction > maxAttraction) {
                maxAttraction = attraction;
                closestCluster = cluster;
            }
        }

        if(closestCluster != null) {
            return closestCluster.center().getRate();
        } else {
            return 0.0;
        }
    }

    //TODO
    public List<String> serializeForOutput() {
        List<String> l = new ArrayList<String>();
        for(HadoopCLCluster<RecordingType> cluster : clusters) {
            l.add(cluster.center().getClass().getName());
            l.add(Integer.toString(cluster.size()));
            List<String> centerTokens = cluster.center().serialize();
            for(String s : centerTokens) {
                l.add(s);
            }
        }
        return l;
    }

    //TODO
    public void initializeFromTokens(List<String> tokens) {
        int i = 0;
        this.clusters.clear();
        while(i < tokens.size()) {
            String centerClassName = tokens.get(i);
            int clusterSize = Integer.parseInt(tokens.get(i+1));

            int j = i + 2;
            List<String> centerTokens = new ArrayList<String>();
            while(j < tokens.size() && tokens.indexOf("org") != 0) {
                centerTokens.add(tokens.get(j));
                j = j + 1;
            }

            if(centerClassName.equals("org.apache.hadoop.mapreduce.HadoopCLDoubleRecording")) {
                HadoopCLDoubleRecording center = new HadoopCLDoubleRecording(centerTokens);
                HadoopCLCluster cluster = new HadoopCLCluster<Double>(center, clusterSize);
                this.clusters.add(cluster);
            } else if(centerClassName.equals("org.apache.hadoop.mapreduce.HadoopCLDoubleRecording")) {
                HadoopCLArrayRecording center = new HadoopCLArrayRecording(centerTokens);
                HadoopCLCluster cluster = new HadoopCLCluster<double[]>(center, clusterSize);
                this.clusters.add(cluster);
            } else {
                throw new RuntimeException("Unsupported center class for cluster: "+centerClassName);
            }
            i = j;
        }
        this.modifiable = false;
    }
}
