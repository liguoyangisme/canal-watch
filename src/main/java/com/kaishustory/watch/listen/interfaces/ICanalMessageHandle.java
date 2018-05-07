package com.kaishustory.watch.listen.interfaces;

import com.alibaba.otter.canal.protocol.Message;

/**
 * Canal订阅消息处理接口
 * @author liguoyang
 * @create 2018-05-06 下午1:46
 **/
public interface ICanalMessageHandle {

    /**
     * 订阅消息处理
     * @param message 数据变更消息
     * @return 是否处理成功
     */
    boolean handle(Message message);
}
