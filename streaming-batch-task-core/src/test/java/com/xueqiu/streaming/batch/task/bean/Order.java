package com.xueqiu.streaming.batch.task.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Order {
    private long id;
    private long uid;
    private int num;
    private String  goodsName;
}
