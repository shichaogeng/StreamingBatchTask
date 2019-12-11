package com.xueqiu.streaming.batch.task.core.task;

import com.xueqiu.streaming.batch.task.core.AbstractBatchTask;

/**
 * deal with data in stream way on multiple dbs and tables
 * @Description:
 * @Author:renxian
 * @Date:2019-12-11
 */
public class MultiSourceBatchTask extends AbstractBatchTask{




    @Override
    protected void execute() {

    }

    @Override
    protected boolean hasData() {
        return true;
    }

    @Override
    protected void notice() {
        super.notice();
    }

    @Override
    protected String getTaskSymbolStr() {
        return "MultiSourceBatchTask "+taskName;
    }
}
