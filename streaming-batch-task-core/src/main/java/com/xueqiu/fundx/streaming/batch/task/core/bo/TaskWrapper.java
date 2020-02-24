package com.xueqiu.fundx.streaming.batch.task.core.bo;


import com.xueqiu.fundx.streaming.batch.task.core.config.GlobalBatchTaskConfig;
import com.xueqiu.fundx.streaming.batch.task.core.context.TaskContextHolder;
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
    @Builder.Default
    private volatile boolean interrupted = false;


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
            this.lastFinishTime = Calendar.getInstance().getTimeInMillis();
            this.executeTimes.getAndIncrement();
            TaskResultInfo taskResultInfo = TaskResultInfo.builder()
                    .startTime(this.startTime)
                    .costTime(this.lastFinishTime - this.startTime)
                    .taskStatus(getResultStatus())
                    .successCount(TaskContextHolder.get().getSuccessNum().intValue())
                    .failedCount(TaskContextHolder.get().getFailedNum().intValue())
                    .build();
            this.statisticData.add(taskResultInfo);
            //初始化了的任务获得了执行权
            if(TaskContextHolder.get().getIsTaskOwner()){
                this.running = false;
            }
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

    public void destroy(){
            this.interrupted=true;
    }


    private GlobalBatchTaskConfig.TaskStatus getResultStatus(){
        if(interrupted){
            return GlobalBatchTaskConfig.TaskStatus.END_INTERRUPTED;
        }
        return TaskContextHolder.get().getTaskStatus();
    }
}
