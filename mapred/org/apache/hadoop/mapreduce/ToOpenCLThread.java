package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import java.util.LinkedList;

public class ToOpenCLThread implements Runnable {
    private static Object lock = new Object();
    public static LinkedList<HadoopCLBuffer> toRunFromMain = null;
    public static LinkedList<HadoopCLBuffer> toRunFromHadoop = null;
    private static boolean fromMain = false;
    private static boolean fromHadoop = false;
    private static int nHadoopTasksActive = 0;

    private final ProfileContext profiler;

    private final HadoopOpenCLContext clContext;

    public ToOpenCLThread(HadoopCLKernel setKernel, ProfileContext setProfiler,
            HadoopOpenCLContext setCLContext) {
        this.profiler = setProfiler;
        this.clContext = setCLContext;
    }

    public static void addWorkFromMain(HadoopCLBuffer toAdd) {
        synchronized(lock) {
            toRunFromMain.add(toAdd);
            lock.notify();
        }
    }

    public static void addWorkFromHadoop(HadoopCLBuffer toAdd) {
        synchronized(lock) {
            toRunFromHadoop.add(toAdd);
            lock.notify();
        }
    }

    public static HadoopCLBuffer getWork() throws InterruptedException {
        HadoopCLBuffer work = null;
        fromMain = false;
        fromHadoop = false;
        synchronized(lock) {
            while(true) {
                if(!toRunFromMain.isEmpty()) {
                    work = toRunFromMain.poll();
                    fromMain = true;
                    break;
                }

                if(!toRunFromHadoop.isEmpty()) {
                    nHadoopTasksActive--;
                    work = toRunFromHadoop.poll();
                    fromHadoop = true;
                    break;
                }
                lock.wait();
            }

        }
        return work;
    }

    private HadoopCLKernel getKernel(HadoopOpenCLContext context, boolean isMapper) {
        if(isMapper) {
            return clContext.getMapperKernel();
        } else {
            return clContext.getReducerKernel();
        }
    }

    @Override
    public void run()  {
        try {
            boolean mainDone = false;
            while(true) {
                HadoopCLBuffer work = getWork();

                if(work == null) {
                    if(fromMain) {
                        mainDone = true;
                    }
                } else {
                    boolean isMapper = work instanceof HadoopCLMapperBuffer;
                    HadoopCLKernel kernel = getKernel(clContext, isMapper);

                    work.fill(kernel);
                    kernel.launchKernel(profiler);

                    while(!work.completedAll()) {
                        HadoopCLBuffer clone = work.cloneIncomplete();
                        ToHadoopThread.addWork(work);
                        work = clone;
                        kernel = getKernel(clContext, isMapper);
                        work.fill(kernel);
                        kernel.launchKernel(profiler);
                    }

                    nHadoopTasksActive++;
                    ToHadoopThread.addWork(work);
                }

                if(mainDone && nHadoopTasksActive == 0) {
                    break;
                }
            }
            ToHadoopThread.addWork(null);
        } catch(Exception e) {
            // not really sure what can be done here...
        }
    }
}
