package com.xueqiu.streaming.batch.task.core;

public interface TaskLifecycle {

    boolean isRunning(String taskName);

    void finish(String taskName);

    void destory(String taskName);
}
