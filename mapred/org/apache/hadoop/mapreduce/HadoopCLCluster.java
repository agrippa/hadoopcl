package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.ArrayList;

public class HadoopCLCluster<RecordingType> {
    private HadoopCLRecording<RecordingType> center;
    private final List<HadoopCLRecording<RecordingType>> members;
    private int clusterSize;
    private final boolean modifiable;

    public HadoopCLCluster(HadoopCLRecording<RecordingType> seed) {
        this.center = seed;
        this.members = new ArrayList<HadoopCLRecording<RecordingType>>();
        this.members.add(seed);
        this.clusterSize = 1;
        this.modifiable = true;
    }

    public HadoopCLCluster(HadoopCLRecording<RecordingType> center,
            int clusterSize) {
        this.center = center;
        this.members = null;
        this.clusterSize = clusterSize;
        this.modifiable = false;
    }

    public double attraction(RecordingType other) {
        double dist = center.distance(other);
        return Math.abs(clusterSize / (dist * dist));
    }

    public double attraction(HadoopCLRecording<RecordingType> other) {
        double dist = center.distance(other);
        return Math.abs(clusterSize / (dist * dist));
    }

    public double attraction(HadoopCLCluster<RecordingType> other){ 
        double dist = center.distance(other.center);
        return Math.abs(this.clusterSize * other.clusterSize / (dist * dist));
    }

    public void addMember(HadoopCLRecording<RecordingType> other) {
        if(!modifiable) {
            throw new RuntimeException("Adding to a non-modifiable cluster");
        }
        this.members.add(other);
        this.center = this.center.mean(this.members);
        this.clusterSize++;
    }

    public void removeAll(List<HadoopCLRecording<RecordingType>> toRemove) {
        for(HadoopCLRecording<RecordingType> r : toRemove) {
            this.members.remove(r);
        }
        this.clusterSize -= toRemove.size();
        this.center = this.center.mean(this.members);
    }

    public void addAll(List<HadoopCLRecording<RecordingType>> toAdd) {
        for(HadoopCLRecording<RecordingType> a : toAdd) {
            this.members.add(a);
        }
        this.clusterSize += toAdd.size();
        this.center = this.center.mean(this.members);
    }

    public void redistributeMembers(HadoopCLCluster<RecordingType> other) {
        List<HadoopCLRecording<RecordingType>> addToMe = new ArrayList<HadoopCLRecording<RecordingType>>();
        List<HadoopCLRecording<RecordingType>> addToOther = new ArrayList<HadoopCLRecording<RecordingType>>();
        for(HadoopCLRecording<RecordingType> mem : other.members) {
            if(this.attraction(mem) > other.attraction(mem)) {
                addToMe.add(mem);
            }
        }
        for(HadoopCLRecording<RecordingType> mem : this.members) {
            if(other.attraction(mem) > this.attraction(mem)) {
                addToOther.add(mem);
            }
        }
        this.removeAll(addToOther);
        other.removeAll(addToMe);

        this.addAll(addToMe);
        other.addAll(addToOther);
    }

    public HadoopCLRecording<RecordingType> center() {
        return this.center;
    }

    public int size() {
        return this.clusterSize;
    }
}
