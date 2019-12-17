package com.xueqiu.fundx.streaming.batch.task.core;


import com.xueqiu.fundx.streaming.batch.task.core.bo.TaskWrapper;

public interface TaskAttributeAccessor<T extends TaskWrapper> {

    /**
     * <ul>
     * <li>方法名称：获取TaskWrapper信息</li>
     * <li>方法介绍：</li>
     * </ul>
     * @author sunyx
     * @date 2019/10/8 17:39
     * @param taskName : 
     * @return : T
     * @throws 
     * @since 
     */
    T getTaskWrapper(String taskName);

    /**
     * <ul>
     * <li>方法名称：封装TaskWrapper信息</li>
     * <li>方法介绍：</li>
     * </ul>
     * @author sunyx
     * @date 2019/10/8 17:39
     * @param taskName : 
     * @param t : 
     * @return : void
     * @throws 
     * @since 
     */
    void setTaskWrapper(String taskName, T t);

    /**
     * <ul>
     * <li>方法名称：判断TaskWrapper是否存在</li>
     * <li>方法介绍：</li>
     * </ul>
     * @author sunyx
     * @date 2019/10/8 17:56
     * @param taskName : 
     * @return : boolean
     * @throws 
     * @since 
     */
    boolean existTaskWrapper(String taskName);
}
