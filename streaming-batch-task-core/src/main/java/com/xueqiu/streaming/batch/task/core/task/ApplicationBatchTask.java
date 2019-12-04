package com.xueqiu.streaming.batch.task.core.task;


import com.xueqiu.streaming.batch.task.core.AbstractBatchTask;
import com.xueqiu.streaming.batch.task.core.handler.impl.StatisticsJobHandler;

public class ApplicationBatchTask extends AbstractBatchTask {

    private static SimpleBatchTask simpleBatchTask;

    private ApplicationBatchTask() {
    }

    public static void run(SimpleTaskConfig taskConfig) {
        simpleBatchTask = SimpleBatchTask.buildTask(taskConfig);
        // 重新设置handler（暂时使用StatisticsJobHandler）
        simpleBatchTask.setJobHandler(new StatisticsJobHandler(simpleBatchTask.getJobHandler()));

        simpleBatchTask.beginTask();
    }

    @Override
    protected void execute() {
        simpleBatchTask.execute();
    }

    @Override
    protected boolean hasData() {
        return simpleBatchTask.hasData();
    }
}
