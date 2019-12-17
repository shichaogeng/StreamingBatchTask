package com.xueqiu.fundx.streaming.batch.task.core.handler.impl;


import com.xueqiu.fundx.streaming.batch.task.core.function.JobContent;
import com.xueqiu.fundx.streaming.batch.task.core.bo.TaskJobResult;
import com.xueqiu.fundx.streaming.batch.task.core.config.GlobalBatchTaskConfig;
import com.xueqiu.fundx.streaming.batch.task.core.handler.JobHandler;
import lombok.Getter;

public class DefaultJobHandler<T> implements JobHandler {

    @Getter
    private JobContent<T> jobContent;

    public DefaultJobHandler(JobContent<T> jobContent) {
        this.jobContent = jobContent;
    }

    @Override
    public TaskJobResult doJobContent(Object o) {
        try {
            jobContent.doNow((T) o);
        } catch (Exception e) {
            return new TaskJobResult(Boolean.FALSE, GlobalBatchTaskConfig.TaskStatus.END_EXCEPTION);
        }
        return new TaskJobResult(Boolean.TRUE, GlobalBatchTaskConfig.TaskStatus.NORMAL);
    }
}
