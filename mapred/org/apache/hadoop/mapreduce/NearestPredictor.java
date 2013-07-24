package org.apache.hadoop.mapreduce;

import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NearestPredictor<RecordingType> implements HadoopCLPredictor<RecordingType> {
    private final List<HadoopCLRecording<RecordingType>> recordings;
    private final ReentrantReadWriteLock lock;
    private final static double MAX_DISTANCE = 0.5;
    private boolean modifiable;
    private static final int CONSIDER = 10;

    public NearestPredictor() {
        this.recordings = new ArrayList<HadoopCLRecording<RecordingType>>();
        this.lock = new ReentrantReadWriteLock();
        this.modifiable = true;
    }

    public void characterizationInitialization(
            List<HadoopCLRecording<RecordingType>> records) {
        this.recordings.addAll(records);
    }

    public void recharacterize(HadoopCLRecording<RecordingType> newRecord,
            List<HadoopCLRecording<RecordingType>> recordings) {
        if(!this.modifiable) {
            throw new RuntimeException("Recharacterizing a non-modifiable predictor (cluster)");
        }
        lock.writeLock().lock();
        this.recordings.add(newRecord);
        lock.writeLock().unlock();
    }

    public double predict(RecordingType other) {
/*
        Iterator<HadoopCLRecording<RecordingType>> iter = sorted.iterator();
        HadoopCLRecording<RecordingType> r = null;
        while(iter.hasNext()) {
            r = iter.next();
            if(r.distance(other) <= MAX_DISTANCE) break;
        }

        double sumClose = 0.0;
        int countClose = 0;

        while(iter.hasNext()) {
            if(r.distance(other) <= MAX_DISTANCE) {
                sumClose = sumClose + r.getRate();
                countClose = countClose + 1;
            } else {
                break;
            }
            r = iter.next();
        }

        if(!iter.hasNext() && r.distance(other) <= MAX_DISTANCE) {
            sumClose = sumClose + r.getRate();
            countClose = countClose + 1;
        }

        if(countClose > 0) {
            return sumClose / countClose; // avg
        } else {
            return 0.0;
        } 
        */
        Set<HadoopCLRecording<RecordingType>> toConsider = new HashSet<HadoopCLRecording<RecordingType>>();
        HadoopCLRecording<RecordingType> farthest = null;
        lock.readLock().lock();
        for(HadoopCLRecording<RecordingType> r : this.recordings) {
            if(toConsider.size() < CONSIDER) {
                toConsider.add(r);
                if(farthest == null || r.distance(other) > farthest.distance(other)) {
                    farthest = r;
                }
            } else if(r.distance(other) < farthest.distance(other)) {
                toConsider.remove(farthest);
                toConsider.add(r);
                farthest = null;
                for(HadoopCLRecording<RecordingType> current : toConsider) {
                    if(farthest == null || current.distance(other) > farthest.distance(other)) {
                        farthest = current;
                    }
                }
            }
        }
        lock.readLock().unlock();
        if(toConsider.size() == 0) {
            return 0.0;
        } else {
            double sumClose = 0.0;
            Iterator<HadoopCLRecording<RecordingType>> iter = toConsider.iterator();
            while(iter.hasNext()) {
                HadoopCLRecording<RecordingType> curr = iter.next();
                sumClose += curr.getRate();
            }
            //for(HadoopCLRecording<RecordingType>> r : toConsider) {
            //    sumClose += r.getRate();
            //}
            return sumClose / toConsider.size();
        }
    }

    public List<String> serializeForOutput() {
        return new ArrayList<String>();
    }

    public void initializeFromTokens(List<String> tokens) {
    }
}
