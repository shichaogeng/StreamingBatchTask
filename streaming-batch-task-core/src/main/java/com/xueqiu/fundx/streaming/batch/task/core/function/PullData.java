package com.xueqiu.fundx.streaming.batch.task.core.function;

import java.util.List;

/**
 * @Description: function to get data
 * @Author:renxian
 * @Date:2019-12-04
 */
@FunctionalInterface
public interface PullData<T> {
    List<T> doNow(Long index, int size);
}