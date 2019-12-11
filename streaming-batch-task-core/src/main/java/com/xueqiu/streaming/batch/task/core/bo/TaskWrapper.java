package com.xueqiu.streaming.batch.task.core.bo;


import com.xueqiu.streaming.batch.task.core.context.TaskContextHolder;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Description:single task wrapper
 * @Author:renxian
 * @Date:2019-12-04
 */
@Data
@Builder
public class TaskWrapper {

    private ExecutorService executorService;
    private String taskName;
    private long startTime;
    private long lastFinishTime;
    @Builder.Default
    private ReentrantLock taskLock = new ReentrantLock();
    @Builder.Default
    private volatile boolean running = false;
    @Builder.Default
    private AtomicInteger executeTimes = new AtomicInteger();
    @Builder.Default
    private final List<TaskResultInfo> statisticData = new ArrayList<>();


    public void start() {
        taskLock.lock();
        try {
            this.running = true;
            this.startTime = Calendar.getInstance().getTimeInMillis();
        } finally {
            taskLock.unlock();
        }
    }

    public void finish() {
        taskLock.lock();
        try {
            this.running = false;
            this.lastFinishTime = Calendar.getInstance().getTimeInMillis();
            this.executeTimes.getAndIncrement();
            TaskResultInfo taskResultInfo = TaskResultInfo.builder()
                    .startTime(this.startTime)
                    .costTime(this.lastFinishTime - this.startTime)
                    .taskStatus(TaskContextHolder.get().getTaskStatus())
                    .successCount(TaskContextHolder.get().getSuccessNum().intValue())
                    .failedCount(TaskContextHolder.get().getFailedNum().intValue())
                    .build();
            this.statisticData.add(taskResultInfo);
        } finally {
            taskLock.unlock();
            //to remove thread local variable to avoid memory leak
            TaskContextHolder.clear();
        }
    }

    public void clearResource() {
        taskLock.lock();
        try {
            if (this.executorService != null) {
                ExecutorService executorService = this.executorService;
                executorService.shutdown();
                if (this.executorService.isShutdown()) {
                    this.executorService = null;//help gc
                }
            }
        } finally {
            taskLock.unlock();
        }
    }
}
