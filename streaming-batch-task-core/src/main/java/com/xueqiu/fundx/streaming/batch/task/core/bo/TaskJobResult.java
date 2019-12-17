package com.xueqiu.fundx.streaming.batch.task.core.bo;


import com.xueqiu.fundx.streaming.batch.task.core.config.GlobalBatchTaskConfig;
import lombok.Data;

@Data
public class TaskJobResult<R> {

    private boolean successFlag;
    private GlobalBatchTaskConfig.TaskStatus taskStatus;
    private R data;

    public TaskJobResult(boolean successFlag, GlobalBatchTaskConfig.TaskStatus taskStatus) {
        this.successFlag = successFlag;
        this.taskStatus = taskStatus;
    }
}