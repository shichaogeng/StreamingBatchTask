package com.xueqiu.streaming.batch.task;

import cn.hutool.core.math.MathUtil;
import cn.hutool.core.util.NumberUtil;
import com.xueqiu.fundx.streaming.batch.task.core.config.PooledResourceStrategy;
import com.xueqiu.fundx.streaming.batch.task.core.context.TaskManageContainer;
import com.xueqiu.fundx.streaming.batch.task.core.task.SimpleBatchTask;
import com.xueqiu.fundx.streaming.batch.task.core.task.SimpleTaskConfig;
import com.xueqiu.streaming.batch.task.bean.Order;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class SimpleBatchTaskTest {

    //test  task get empty data
    @Test
    public void noDataTest() {
        for (int i = 0; i < 3; i++) {
            SimpleTaskConfig<String> simpleTaskConfig = SimpleTaskConfig.<String>builder()
                    .taskName("noDataTest")
                    .size(1000)
                    .threadNum(2)
                    .strategy(PooledResourceStrategy.CREATE_DESTROY)
                    .indexInfo(r -> null)
                    .identifier(r -> r)
                    .pullData((index, size) -> Collections.emptyList())
                    .jobContent((e) -> {
                        try {
                            Thread.sleep(1000 * 10);
                        } catch (InterruptedException e1) {
                        }
                        return true;
                    })
                    .build();
            SimpleBatchTask.buildTask(simpleTaskConfig).beginTask();
        }
    }

    //test  task in serial
    @Test
    public void getResourceTest() {
        for (int i = 0; i < 3; i++) {
            SimpleTaskConfig<String> simpleTaskConfig = SimpleTaskConfig.<String>builder()
                    .taskName("getResourceTest")
                    .size(1000)
                    .threadNum(2)
                    .strategy(PooledResourceStrategy.CREATE_DESTROY)
                    .indexInfo(r -> null)
                    .identifier(r -> r)
                    .pullData((index, size) -> Arrays.asList("111", "222"))
                    .jobContent((e) -> {
                        try {
                            Thread.sleep(1000 * 10);
                        } catch (InterruptedException e1) {
                        }
                        return true;
                    })
                    .build();
            SimpleBatchTask.buildTask(simpleTaskConfig).beginTask();
        }
    }

    //test  task resource restriction in parallel
    @Test
    public void checkResourceTest() {
        CountDownLatch countDownLatch = new CountDownLatch(20);
        for (int i = 0; i < 20; i++) {
            final int index=i%5;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    SimpleTaskConfig<String> simpleTaskConfig = SimpleTaskConfig.<String>builder()
                            .taskName("checkResourceTest"+index)
                            .size(1000)
                            .threadNum(36)
                            .strategy(PooledResourceStrategy.CREATE_DESTROY)
                            .indexInfo(r -> null)
                            .identifier(r -> r)
                            .pullData((index, size) -> Arrays.asList("111", "222"))
                            .jobContent((e) -> {
                                try {
                                    Thread.sleep(1000 * 60);
                                } catch (InterruptedException e1) {
                                }
                                return true;
                            })
                            .build();
                    SimpleBatchTask.buildTask(simpleTaskConfig).beginTask();
                    countDownLatch.countDown();
                }
            }).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {

        }
    }

    //test  task in groupSerial
    @Test
    public void testGroupSerial() {
        List<Order> datas = new ArrayList<>();
        datas.add(Order.builder().id(1l).uid(111).goodsName("数据线").num(1).build());
        datas.add(Order.builder().id(2l).uid(222).goodsName("苹果手机").num(1).build());
        datas.add(Order.builder().id(3l).uid(111).goodsName("苹果").num(2).build());
        datas.add(Order.builder().id(4l).uid(555).goodsName("茶杯").num(1).build());
        datas.add(Order.builder().id(5l).uid(666).goodsName("维生素B2").num(10).build());
        datas.add(Order.builder().id(6l).uid(555).goodsName("DELL显示器").num(1).build());
        datas.add(Order.builder().id(7l).uid(555).goodsName("macAir").num(1).build());
        datas.add(Order.builder().id(8l).uid(333).goodsName("抽纸").num(8).build());
        datas.add(Order.builder().id(9l).uid(444).goodsName("背包").num(1).build());

        SimpleTaskConfig<Order> simpleTaskConfig = SimpleTaskConfig.<Order>builder()
                .taskName("testGroupSerial")
                .size(1000)
                .threadNum(5)
                .strategy(PooledResourceStrategy.CREATE_DESTROY)
                .indexInfo(r -> r.getId())
                .identifier(r -> String.valueOf(r.getId()))
                .pullData((index, size) -> datas)
                .jobContent((e) -> {
                    System.out.println(e);
                    return true;
                })
                .groupSerial(true)
                .grouping(r -> r.getUid())
                .build();
        SimpleBatchTask.buildTask(simpleTaskConfig).beginTask();

    }

    @Test
    public void testDestroy(){
        CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                SimpleTaskConfig<String> simpleTaskConfig = SimpleTaskConfig.<String>builder()
                        .taskName("testDestroy")
                        .size(1000)
                        .threadNum(2)
                        .strategy(PooledResourceStrategy.CREATE_DESTROY)
                        .indexInfo(r -> null)
                        .identifier(r -> r)
                        .pullData((index, size) -> Arrays.asList("111", "222", "333", "444", "555", "666"))
                        .jobContent((e) -> {
                            try {
                                Thread.sleep(1000 * 10);
                            } catch (InterruptedException e1) {
                            }
                            return true;
                        })
                        .build();
                SimpleBatchTask.buildTask(simpleTaskConfig).beginTask();
                countDownLatch.countDown();
            }
        }).start();
        try {
            Thread.currentThread().sleep(15*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //terminate task manually
        TaskManageContainer.TaskManageContainerFactory.getInstance().destroy("testDestroy");
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
        }
    }

    /**
     * 任务开始前不校验是否存在数据hasData()
     */
    @Test
    public void noHasDataCheckTest() {
        SimpleTaskConfig<String> simpleTaskConfig=new SimpleTaskConfig<String>().toBuilder()
                .taskName("noHasDataCheckTest")
                .size(1000)
                .threadNum(2)
                .strategy(PooledResourceStrategy.CREATE_DESTROY)
                .indexInfo(r -> null)
                .identifier(r -> r)
                .pullData((index, size) -> Collections.emptyList())
                .dataCheck(false)
                .jobContent((e) -> {
                    try {
                        Thread.sleep(1000 * 10);
                    } catch (InterruptedException e1) {
                    }
                    return true;
                })
                .build();
            SimpleBatchTask.buildTask(simpleTaskConfig).beginTask();
    }


    /**
     * 单次拉取数据
     */
    @Test
    public void oncePullDataTest() {
        SimpleTaskConfig<String> simpleTaskConfig=new SimpleTaskConfig<String>().toBuilder()
                .taskName("oncePullDataTest")
                .size(3)
                .threadNum(2)
                .onceDataPull(true)
                .strategy(PooledResourceStrategy.CREATE_DESTROY)
                .indexInfo(r -> null)
                .identifier(r -> r)
                .pullData((index, size) ->  Arrays.asList("111", "222", "333", "444", "555", "666"))
                .dataCheck(false)
                .jobContent((e) -> {

                    return true;
                })
                .build();
        SimpleBatchTask.buildTask(simpleTaskConfig).beginTask();
    }



}
