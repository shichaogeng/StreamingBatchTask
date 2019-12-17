package com.xueqiu.fundx.streaming.batch.task.core.context;



import com.xueqiu.fundx.streaming.batch.task.core.AbstractBatchTask;
import com.xueqiu.fundx.streaming.batch.task.core.bo.TaskResultInfo;
import com.xueqiu.fundx.streaming.batch.task.core.bo.TaskWrapper;
import com.xueqiu.fundx.streaming.batch.task.core.TaskAttributeAccessor;
import com.xueqiu.fundx.streaming.batch.task.core.TaskLifecycle;
import com.xueqiu.fundx.streaming.batch.task.core.config.GlobalBatchTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description: container to manage all tasks on one single instance
 * @Author:renxian
 * @Date:2019-12-04
 */
public class TaskManageContainer implements TaskAttributeAccessor<TaskWrapper>, TaskLifecycle {

    public static final Logger logger = LoggerFactory.getLogger(AbstractBatchTask.class);

    /**
     * 实现{@link TaskAttributeAccessor}接口，封装所有的任务信息，由该容器统一控制
     * <p>从容器中拿取任务{@link #getTaskWrapper(String) getTaskWrapper}
     * <p>设置任务到该容器中{@link #setTaskWrapper(String, TaskWrapper)}  setTaskWrapper}
     * <p>判断任务是否存在{@link #existTaskWrapper(String)}  existTaskWrapper}
     */
    private ConcurrentHashMap<String, TaskWrapper> cachedTask = new ConcurrentHashMap<>();

    private TaskManageContainer() {
    }

    @Override
    public TaskWrapper getTaskWrapper(String taskName) {
        return cachedTask.get(taskName);
    }

    @Override
    public void setTaskWrapper(String taskName, TaskWrapper taskWrapper) {
        cachedTask.put(taskName, taskWrapper);
    }

    @Override
    public boolean existTaskWrapper(String taskName) {
        return cachedTask.contains(taskName);
    }

    @Override
    public boolean isRunning(String taskName) {
        return existTaskWrapper(taskName);
    }

    @Override
    public void finish(String taskName) {
        finishTask(taskName);
    }

    @Override
    public void destory(String taskName) {
    }

    public boolean checkTask(String taskName) {
        TaskWrapper taskWrapper = cachedTask.get(taskName);
        try {
            if (taskWrapper == null) {
                taskWrapper = TaskWrapper.builder().taskName(taskName).build();
                // 双重检查
                TaskWrapper previousTaskWrapper = cachedTask.putIfAbsent(taskName, taskWrapper);
                if (previousTaskWrapper != null) {
                    taskWrapper = previousTaskWrapper;
                }
            }
            taskWrapper.getTaskLock().lock();
            if (taskWrapper.isRunning()) {
                TaskContextHolder.setStatus(GlobalBatchTaskConfig.TaskStatus.IS_RUNNING);
                logger.info("BatchTask-" + taskName + " is in processing");
                return false;
            }
            taskWrapper.setRunning(true);
            TaskContextHolder.get().setIsTaskOwner(true);
        } finally {
            taskWrapper.getTaskLock().unlock();
        }
        return true;
    }

    public void finishTask(String taskName) {
        cachedTask.get(taskName).finish();
    }

    public void clearTaskResource() {
        if (logger.isDebugEnabled()) {
            logger.debug("BatchTask clear task start.");
        }
        Map<String, TaskWrapper> snapShotMap = Collections.unmodifiableMap(cachedTask);
        long nowTime = Calendar.getInstance().getTimeInMillis();
        snapShotMap.forEach((k, v) -> {
            v.getTaskLock().lock();
            try {
                if (!v.isRunning() && nowTime - v.getLastFinishTime() > GlobalBatchTaskConfig.POOL_RESOURCE_MAX_IDLE_TIME) {
                    v.clearResource();
                    if (logger.isDebugEnabled()) {
                        logger.debug("BatchTask cleared task[" + v.getTaskName() + "] pool resource");
                    }
                }
                List<TaskResultInfo> waitToDelete = new ArrayList<>();
                v.getStatisticData().stream().forEach((e) -> {
                    if (nowTime - e.getStartTime() > GlobalBatchTaskConfig.STATISTICS_KEEP_TIME) {
                        waitToDelete.add(e);
                    }
                });
                v.getStatisticData().removeAll(waitToDelete);
            } finally {
                v.getTaskLock().unlock();
            }
        });
    }


  public  static class TaskManageContainerFactory {

        public static final TaskManageContainer INSTANCE = new TaskManageContainer();

        public static TaskManageContainer getInstance() {
            return INSTANCE;
        }
    }
}
