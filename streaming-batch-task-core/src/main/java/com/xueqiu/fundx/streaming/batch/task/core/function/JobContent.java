package com.xueqiu.fundx.streaming.batch.task.core.function;

/**
 * @Description: task main job content
 * @Author:renxian
 * @Date:2019-12-04
 */
@FunctionalInterface
public interface JobContent<T> {

    boolean doNow(T obj);
}
