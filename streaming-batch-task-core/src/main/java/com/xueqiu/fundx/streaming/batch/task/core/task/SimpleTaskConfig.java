package com.xueqiu.fundx.streaming.batch.task.core.task;


import cn.hutool.core.lang.Assert;
import com.xueqiu.fundx.streaming.batch.task.core.annotation.TaskIndex;
import com.xueqiu.fundx.streaming.batch.task.core.annotation.TaskKey;
import com.xueqiu.fundx.streaming.batch.task.core.config.BaseTaskConfig;
import com.xueqiu.fundx.streaming.batch.task.core.config.PooledResourceStrategy;
import com.xueqiu.fundx.streaming.batch.task.core.function.JobContent;
import com.xueqiu.fundx.streaming.batch.task.core.function.PullData;
import com.xueqiu.fundx.streaming.batch.task.core.bo.TaskSettingWrapper;
import com.xueqiu.fundx.streaming.batch.task.core.config.GlobalBatchTaskConfig;
import lombok.Builder;
import lombok.Data;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * @Description:
 * @Author:renxian
 * @Date:2019-12-04
 */
@Builder
@Data
public class SimpleTaskConfig<T> extends BaseTaskConfig {

    private PullData<T> pullData;

    private JobContent<T> jobContent;

    private Function<T, Long> indexInfo;

    private Function<T, String> identifier;

    private int size;

    private String taskName;

    private int threadNum;

    private ExecutorService executorService;

    @Builder.Default
    private int shardIndex=-1;

    @Builder.Default
    private int shardNum=-1;

    @Builder.Default
    private PooledResourceStrategy strategy = PooledResourceStrategy.COMMON;

    public static SimpleTaskConfig buildTaskCheck(SimpleTaskConfig taskConfig){
        Assert.notNull(taskConfig.getJobContent(), "SimpleTaskConfig.jobContent is null");
        Assert.notNull(taskConfig.getPullData(), "SimpleTaskConfig.pullData is null");
        Assert.notNull(taskConfig.getTaskName(), "SimpleTaskConfig.taskName is null");

        int size = taskConfig.getSize();
        if (size <= 0) {
            taskConfig.size = GlobalBatchTaskConfig.DEFAULT_BATCH_SIZE;
        } else if (size > GlobalBatchTaskConfig.MAX_BATCH_SIZE) {
            taskConfig.size = GlobalBatchTaskConfig.MAX_BATCH_SIZE;
        }
        int threadNum = taskConfig.getThreadNum();
        if (threadNum <= 0) {
            taskConfig.threadNum = GlobalBatchTaskConfig.DEFAULT_THREAD_NUM;
        } else if (threadNum > GlobalBatchTaskConfig.MAX_THREAD_NUM) {
            taskConfig.threadNum = GlobalBatchTaskConfig.MAX_THREAD_NUM;
        }

        if (PooledResourceStrategy.CUSTOM.equals(taskConfig.getStrategy()) && taskConfig.getExecutorService() == null) {
            throw new IllegalArgumentException("executorService can't be null when choose custom pool strategy");
        }
        // 通过注解获取到identifier和indexInfo方法（静态配置）
        if (taskConfig.getIndexInfo() == null && taskConfig.getCls() != null) {
            TaskSettingWrapper indexWrapper = new TaskSettingWrapper(taskConfig.getCls(), TaskIndex.class, Arrays.asList("long", "java.lang.Long"));
            if (indexWrapper.isUsable()) {
                taskConfig.setIndexInfo(t -> indexWrapper.getSettingInfo(t, Long.class));
            }
        }
        if (taskConfig.getIdentifier() == null && taskConfig.getCls() != null) {
            TaskSettingWrapper keyWrapper = new TaskSettingWrapper(taskConfig.getCls(), TaskKey.class, Arrays.asList("java.lang.String"));
            if (keyWrapper.isUsable()) {
                taskConfig.setIdentifier(t -> keyWrapper.getSettingInfo(t, String.class));
            }
        }
        return taskConfig;
    }
}
