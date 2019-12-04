package com.xueqiu.streaming.batch.task.core.task;


import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.lang.Assert;
import com.xueqiu.streaming.batch.task.core.AbstractBatchTask;
import com.xueqiu.streaming.batch.task.core.annotation.TaskIndex;
import com.xueqiu.streaming.batch.task.core.annotation.TaskKey;
import com.xueqiu.streaming.batch.task.core.bo.TaskJobResult;
import com.xueqiu.streaming.batch.task.core.bo.TaskSettingWrapper;
import com.xueqiu.streaming.batch.task.core.config.GlobalBatchTaskConfig;
import com.xueqiu.streaming.batch.task.core.config.PooledResourceStrategy;
import com.xueqiu.streaming.batch.task.core.context.TaskContextHolder;
import com.xueqiu.streaming.batch.task.core.function.JobContent;
import com.xueqiu.streaming.batch.task.core.function.PullData;
import com.xueqiu.streaming.batch.task.core.handler.JobHandler;
import com.xueqiu.streaming.batch.task.core.handler.impl.DefaultJobHandler;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @Description:
 * @Author:renxian
 * @Date:2019-12-04
 */
public class SimpleBatchTask<T> extends AbstractBatchTask {

    public static final Logger logger = LoggerFactory.getLogger(SimpleBatchTask.class);

    private int size;

    private Long startIndex = 0l;

    private PullData<T> pullData;

    @Setter
    @Getter
    private JobHandler<T> jobHandler;

    private Function<T, Long> indexInfo;

    private Function<T, String> identifier;

    private CountDownLatch countDownLatch = new CountDownLatch(0);

    protected Vector<String> failedRecord = new Vector<>();

    public static SimpleBatchTask buildTask(SimpleTaskConfig taskConfig) {
        Assert.notNull(taskConfig.getJobContent(), "SimpleTaskConfig.jobContent");
        Assert.notNull(taskConfig.getPullData(), "SimpleTaskConfig.pullData");
        Assert.notNull(taskConfig.getTaskName(), "SimpleTaskConfig.taskName");

        int size = taskConfig.getSize();
        if (size <= 0) {
            size = GlobalBatchTaskConfig.DEFAULT_BATCH_SIZE;
        } else if (size > GlobalBatchTaskConfig.MAX_BATCH_SIZE) {
            size = GlobalBatchTaskConfig.MAX_BATCH_SIZE;
        }

        int threadNum = taskConfig.getThreadNum();
        if (threadNum <= 0) {
            threadNum = GlobalBatchTaskConfig.DEFAULT_THREAD_NUM;
        } else if (threadNum > GlobalBatchTaskConfig.MAX_THREAD_NUM) {
            threadNum = GlobalBatchTaskConfig.MAX_THREAD_NUM;
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

        SimpleBatchTask simpleBatchTask = new SimpleBatchTask(size, taskConfig.getPullData(), taskConfig.getJobContent(), taskConfig.getTaskName(), threadNum, taskConfig.getStrategy(), taskConfig.getExecutorService());
        simpleBatchTask.indexInfo = taskConfig.getIndexInfo();
        simpleBatchTask.identifier = taskConfig.getIdentifier();

        return simpleBatchTask;
    }

    private SimpleBatchTask(int size, PullData<T> pullData, JobContent<T> jobContent, String taskName, int threadNum, PooledResourceStrategy strategy, ExecutorService executorService) {
        super(taskName, threadNum, strategy, executorService);
        this.size = size;
        this.pullData = pullData;
        if (jobHandler == null) {
            this.jobHandler = new DefaultJobHandler(jobContent);
        }
    }

    @Override
    protected String getTaskSymbolStr() {
        return "SimpleBatchTask " + taskName;
    }

    @Override
    protected void execute() {
        Long index = startIndex;
        AtomicInteger successNum = TaskContextHolder.get().getSuccessNum();
        AtomicInteger failedNum = TaskContextHolder.get().getFailedNum();
        while (true) {
            List<T> data = null;
            try {
                data = pullData.doNow(index, size);
            } catch (Exception e) {
                logger.info("{} pull data exception,index={},size={}", getTaskSymbolStr(),index,size);
            }
            if (CollectionUtil.isEmpty(data)) {
                break;
            }
            // 前置预处理
            pre(data.get(0));

            this.countDownLatch = new CountDownLatch(data.size());
            data.stream().forEach((e) -> executorService.submit(() -> {
                        try {
                            if(logger.isDebugEnabled()){
                                logger.debug(getTaskSymbolStr()+"handle data "+identifier.apply(e));
                            }
                            TaskJobResult result = this.jobHandler.doJobContent(e);
                            if (result.isSuccessFlag()) {
                                successNum.getAndIncrement();
                            } else {
                                failedNum.getAndIncrement();
                                failedRecord.add(identifier.apply(e));
                            }
                        } catch (Exception ex) {
                            failedRecord.add(identifier.apply(e));
                            failedNum.getAndIncrement();
                        } finally {
                            this.countDown();
                        }
                    })
            );
            this.await();
            index = indexInfo.apply(data.get(data.size() - 1));
            if (data.size() < size)
                break;
        }
        logger.info("{} finished,failed_num={}" , getTaskSymbolStr(),failedRecord.size());
    }


    //拉取的第一批数据非空
    @Override
    protected boolean hasData() {
        return CollectionUtil.isNotEmpty(pullData.doNow(startIndex, size));
    }

    private void countDown() {
        this.countDownLatch.countDown();
    }

    private void await() {
        try {
            this.countDownLatch.await();
        } catch (InterruptedException e) {
        }
    }

    private void pre(T data) {
        // 如果还是没有则动态获取
        if (identifier == null) {
            TaskSettingWrapper keyWrapper = new TaskSettingWrapper(data.getClass(), TaskKey.class, Arrays.asList("java.lang.String"));
            if (keyWrapper.isUsable()) {
                this.identifier = t -> keyWrapper.getSettingInfo(t, String.class);
            }
        }
        if (indexInfo == null) {
            TaskSettingWrapper indexWrapper = new TaskSettingWrapper(data.getClass(), TaskIndex.class, Arrays.asList("long", "java.lang.Long"));
            if (indexWrapper.isUsable()) {
                this.indexInfo = t -> indexWrapper.getSettingInfo(t, Long.class);
            }
        }
    }
}
