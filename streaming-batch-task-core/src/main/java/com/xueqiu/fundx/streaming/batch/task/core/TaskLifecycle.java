package com.xueqiu.fundx.streaming.batch.task.core;

public interface TaskLifecycle {

    boolean isRunning(String taskName);

    @Deprecated
    //finish can't be exposed for reason of safety
    void finish(String taskName);

    void destroy(String taskName);
}
