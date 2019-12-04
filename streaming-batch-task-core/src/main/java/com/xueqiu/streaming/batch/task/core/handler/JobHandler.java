package com.xueqiu.streaming.batch.task.core.handler;


import com.xueqiu.streaming.batch.task.core.bo.TaskJobResult;
import com.xueqiu.streaming.batch.task.core.function.JobContent;

public interface JobHandler<T> {

    TaskJobResult<? extends Object> doJobContent(T t);

    JobContent<T> getJobContent();
}
