package com.xueqiu.streaming.batch.task.core.handler.impl;


import com.xueqiu.streaming.batch.task.core.bo.TaskJobResult;
import com.xueqiu.streaming.batch.task.core.config.GlobalBatchTaskConfig;
import com.xueqiu.streaming.batch.task.core.function.JobContent;
import com.xueqiu.streaming.batch.task.core.handler.JobHandler;
import lombok.Getter;

public class StatisticsJobHandler<T, R> implements JobHandler {

    @Getter
    private JobContent<T> jobContent;

    private JobContent<R> statisticsContent;

    public StatisticsJobHandler(JobHandler jobHandler) {
        this.jobContent = jobHandler.getJobContent();
    }

    @Override
    public TaskJobResult doJobContent(Object o) {
        try {
            jobContent.doNow((T) o);
            // 统计
            statisticsContent.doNow(null);
            // 发布事件
//            publish();
        } catch (Exception e) {
            return new TaskJobResult(Boolean.FALSE, GlobalBatchTaskConfig.TaskStatus.END_EXCEPTION);
        }
        return new TaskJobResult(Boolean.TRUE, GlobalBatchTaskConfig.TaskStatus.NORMAL);
    }
}
