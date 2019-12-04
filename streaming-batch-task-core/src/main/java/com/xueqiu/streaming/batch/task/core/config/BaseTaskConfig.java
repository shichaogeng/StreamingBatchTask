package com.xueqiu.streaming.batch.task.core.config;

import lombok.Data;

//@AllArgsConstructor
@Data
public class BaseTaskConfig<T> {

//    private String taskName;
//
//    private int size;
//
//    private int threadNum;
//
//    private PullData<T> pullData;
//
//    private JobContent<T> jobContent;
//
//    private ExecutorService executorService;

    private Class<T> cls;

    public BaseTaskConfig() {
    }
}
