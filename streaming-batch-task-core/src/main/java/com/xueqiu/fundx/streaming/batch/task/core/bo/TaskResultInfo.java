package com.xueqiu.fundx.streaming.batch.task.core.bo;


import com.xueqiu.fundx.streaming.batch.task.core.config.GlobalBatchTaskConfig;
import lombok.Builder;
import lombok.Data;

/**
 * @Description: simple task result info
 * @Author:renxian
 * @Date:2019-12-04
 */
@Data
@Builder
public class TaskResultInfo {

    private long startTime;
    private long costTime;
    private int successCount;
    private int failedCount;
    private GlobalBatchTaskConfig.TaskStatus taskStatus;
}
