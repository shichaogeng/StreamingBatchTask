package com.xueqiu.fundx.streaming.batch.task.core.task;


import cn.hutool.core.collection.CollectionUtil;
import com.xueqiu.fundx.streaming.batch.task.core.annotation.TaskIndex;
import com.xueqiu.fundx.streaming.batch.task.core.annotation.TaskKey;
import com.xueqiu.fundx.streaming.batch.task.core.bo.TaskJobResult;
import com.xueqiu.fundx.streaming.batch.task.core.config.PooledResourceStrategy;
import com.xueqiu.fundx.streaming.batch.task.core.function.JobContent;
import com.xueqiu.fundx.streaming.batch.task.core.function.PullData;
import com.xueqiu.fundx.streaming.batch.task.core.AbstractBatchTask;
import com.xueqiu.fundx.streaming.batch.task.core.bo.TaskSettingWrapper;
import com.xueqiu.fundx.streaming.batch.task.core.context.TaskContextHolder;
import com.xueqiu.fundx.streaming.batch.task.core.handler.JobHandler;
import com.xueqiu.fundx.streaming.batch.task.core.handler.impl.DefaultJobHandler;
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

    public static <T> SimpleBatchTask buildTask(SimpleTaskConfig<T> taskConfig) {
        SimpleTaskConfig.buildTaskCheck(taskConfig);
        SimpleBatchTask simpleBatchTask = new SimpleBatchTask(taskConfig.getSize(), taskConfig.getPullData(), taskConfig.getJobContent(), taskConfig.getTaskName(), taskConfig.getThreadNum(), taskConfig.getStrategy(), taskConfig.getExecutorService());
        simpleBatchTask.indexInfo = taskConfig.getIndexInfo();
        simpleBatchTask.identifier = taskConfig.getIdentifier();
        return simpleBatchTask;
    }

    protected SimpleBatchTask() {
    }

    protected SimpleBatchTask(SimpleTaskConfig taskConfig) {
        this(taskConfig.getSize(), taskConfig.getPullData(), taskConfig.getJobContent(), taskConfig.getTaskName(), taskConfig.getThreadNum(), taskConfig.getStrategy(), taskConfig.getExecutorService());
        this.indexInfo = taskConfig.getIndexInfo();
        this.identifier = taskConfig.getIdentifier();
    }

    protected SimpleBatchTask(int size, PullData<T> pullData, JobContent<T> jobContent, String taskName, int threadNum, PooledResourceStrategy strategy, ExecutorService executorService) {
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
        streamUnit(startIndex);
    }

    protected void streamUnit(Long startIndex){
        Long index=startIndex;
        while (true) {
            AtomicInteger successNum = TaskContextHolder.get().getSuccessNum();
            AtomicInteger failedNum = TaskContextHolder.get().getFailedNum();
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
                                logger.debug(getTaskSymbolStr()+" handle data "+identifier.apply(e));
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
