package com.xueqiu.fundx.streaming.batch.task.core.task;


import cn.hutool.core.collection.CollectionUtil;
import com.xueqiu.fundx.streaming.batch.task.core.AbstractBatchTask;
import com.xueqiu.fundx.streaming.batch.task.core.annotation.TaskIndex;
import com.xueqiu.fundx.streaming.batch.task.core.annotation.TaskKey;
import com.xueqiu.fundx.streaming.batch.task.core.bo.TaskJobResult;
import com.xueqiu.fundx.streaming.batch.task.core.bo.TaskSettingWrapper;
import com.xueqiu.fundx.streaming.batch.task.core.config.PooledResourceStrategy;
import com.xueqiu.fundx.streaming.batch.task.core.context.TaskContext;
import com.xueqiu.fundx.streaming.batch.task.core.context.TaskContextHolder;
import com.xueqiu.fundx.streaming.batch.task.core.function.JobContent;
import com.xueqiu.fundx.streaming.batch.task.core.function.PullData;
import com.xueqiu.fundx.streaming.batch.task.core.handler.JobHandler;
import com.xueqiu.fundx.streaming.batch.task.core.handler.impl.DefaultJobHandler;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private Function<T,Object> grouping;

    private CountDownLatch countDownLatch = new CountDownLatch(0);

    protected Vector<String> failedRecord = new Vector<>();

    private boolean groupSerial=false;

    private boolean dataCheck=true;

    private boolean onceDataPull=false;

    public static <T> SimpleBatchTask buildTask(SimpleTaskConfig<T> taskConfig) {
        SimpleTaskConfig.buildTaskCheck(taskConfig);
        SimpleBatchTask simpleBatchTask = new SimpleBatchTask(taskConfig.getSize(), taskConfig.getPullData(), taskConfig.getJobContent(), taskConfig.getTaskName(), taskConfig.getThreadNum(), taskConfig.getStrategy(), taskConfig.getExecutorService());
        simpleBatchTask.indexInfo = taskConfig.getIndexInfo();
        simpleBatchTask.identifier = taskConfig.getIdentifier();
        simpleBatchTask.grouping=taskConfig.getGrouping();
        simpleBatchTask.groupSerial=taskConfig.isGroupSerial();
        simpleBatchTask.dataCheck=taskConfig.isDataCheck();
        simpleBatchTask.onceDataPull=taskConfig.isOnceDataPull();
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
        TaskContext taskContext=TaskContextHolder.get();
        while (true) {
            List<T> data = null;
            if(taskContext.isInterrupted()){
                logger.info("{} is terminated by destroy method", getTaskSymbolStr());
                break;
            }
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
            //支持分组后串行处理
            if(groupSerial){
                Map<Object,List<T>> groupMap=data.stream().collect(Collectors.groupingBy(grouping));
                this.countDownLatch = new CountDownLatch(groupMap.size());
                groupMap.forEach((k,v) -> executorService.submit(() ->v.stream().forEach((e)->doSingleDataContent(e,taskContext))));
            }else{
                this.countDownLatch = new CountDownLatch(data.size());
                data.stream().forEach((e) -> executorService.submit(()->doSingleDataContent(e,taskContext)));
            }
            this.await();
            index = indexInfo.apply(data.get(data.size() - 1));
            if (data.size() < size||this.onceDataPull)
                break;
        }
    }

    //拉取的第一批数据非空
    @Override
    protected boolean hasData() {
        return !dataCheck||CollectionUtil.isNotEmpty(pullData.doNow(startIndex, size));
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

    //minimum data handle unit
    private void doSingleDataContent(T t, TaskContext taskContext){
        try {
            if(taskContext.isInterrupted()){
                return;//just return;
            }
            if(logger.isDebugEnabled()){
                logger.debug(getTaskSymbolStr()+" handle data "+identifier.apply(t));
            }
            TaskJobResult result = this.jobHandler.doJobContent(t);
            if (result.isSuccessFlag()) {
                taskContext.getSuccessNum().getAndIncrement();
            } else {
                taskContext.getFailedNum().getAndIncrement();
                failedRecord.add(identifier.apply(t));
            }
        } catch (Exception ex) {
            failedRecord.add(identifier.apply(t));
            taskContext.getFailedNum().getAndIncrement();
        } finally {
            this.countDown();
        }
    }

}
