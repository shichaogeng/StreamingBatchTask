package com.xueqiu.fundx.streaming.batch.task.core.task;

import cn.hutool.core.lang.Assert;
import com.xueqiu.fundx.sharding.client.read.core.bean.SplitDbTableSession;
import com.xueqiu.fundx.sharding.client.read.core.context.SplitDbTableContext;
import com.xueqiu.fundx.sharding.client.read.core.context.SplitDbTableSessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * deal with data in stream way on multiple dbs and tables
 *
 * @Description:
 * @Author:renxian
 * @Date:2019-12-11
 */
public class MultiSourceBatchTask extends SimpleBatchTask {

    public static final Logger logger = LoggerFactory.getLogger(MultiSourceBatchTask.class);


    private Class<?> clazz;

    private int shardIndex;

    private int shardNum;

    public static  MultiSourceBatchTask buildTask(SimpleTaskConfig taskConfig) {
        Assert.notNull(taskConfig.getCls(), "MultiSourceBatchTask.cls is null");
        if (taskConfig.getShardNum() > 0) {
            Assert.isTrue(taskConfig.getShardIndex() >= 0 && taskConfig.getShardIndex() < taskConfig.getShardNum()
                    , "MultiSourceBatchTask shardIndex is incorrect,shardIndex={},shardNum={}", taskConfig.getShardIndex(), taskConfig.getShardNum());
        }
        SimpleTaskConfig.buildTaskCheck(taskConfig);
        return new MultiSourceBatchTask(taskConfig);
    }

    private MultiSourceBatchTask(SimpleTaskConfig taskConfig) {
        super(taskConfig);
        this.clazz = taskConfig.getCls();
        this.shardIndex = taskConfig.getShardIndex();
        this.shardNum=taskConfig.getShardNum();
    }

    @Override
    protected void execute() {
        List<SplitDbTableSession> sessions = SplitDbTableContext.getSessions(clazz);
        if (sessions == null) {
            logger.error("{} get SplitDbTable config failed,please check you @SplitDbTable config");
            return;
        }
        for (int i = 0; i < sessions.size(); i++) {
            if (isHitShard(i)) {
                logger.info("MultiSourceBatchTask hit shard,shardNum={},shardIndex={},elementIndex={},SplitDbTableSession={}"
                        , this.shardNum, this.shardIndex, i, sessions.get(i));
                SplitDbTableSessionHolder.set(sessions.get(i));
                super.streamUnit(0l);
                logger.info("MultiSourceBatchTask SplitDbTableSession={} finished streamUnit", sessions.get(i));
            }
        }

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
        return "MultiSourceBatchTask " + taskName;
    }

    private boolean isHitShard(int elementIndex) {
        //不分片||等于shardIndex
        if (this.shardNum <= 0 ||elementIndex%this.shardNum == shardIndex) {
            return true;
        }
        return false;
    }
}
