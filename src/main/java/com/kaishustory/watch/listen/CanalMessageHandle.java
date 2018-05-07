package com.kaishustory.watch.listen;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.kaishustory.watch.listen.interfaces.ICanalMessageHandle;
import com.kaishustory.watch.listen.model.RowChangeInfo;
import com.kaishustory.watch.listen.model.TransEvent;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Canal订阅消息处理
 *
 * @author liguoyang
 * @create 2018-05-06 下午1:44
 **/
@Slf4j
public class CanalMessageHandle implements ICanalMessageHandle {

    /**
     * 订阅消息处理
     * @param message 数据变更消息
     * @return 是否处理成功
     */
    @Override
    public boolean handle(Message message) {

        try{
            //获取事务列表
            List<TransEvent> transList = getTransactionList(message);

            //事务处理
            transList.stream().filter(t -> t.hasChange()).forEach(trans -> {
                trans.getRowChanges().forEach(change -> {
                    //key = 数据库.表名
                    String key = change.getHeader().getSchemaName()+"."+change.getHeader().getTableName();
                    //检查对应表是否有定义监听
                    if(CanalListener.listenChangeEvent.containsKey(key)) {
                        //事件类型
                        final CanalEntry.EventType eventType = change.getHeader().getEventType();

                        //新增事件处理
                        if(eventType == CanalEntry.EventType.INSERT){
                            change.getRowChange().getRowDatasList().forEach(row -> {
                                CanalListener.listenChangeEvent.get(key).getInsertListeners().forEach(bean -> {
                                    try {
                                        bean.insertHandle(change.getHeader().getSchemaName(),change.getHeader().getTableName(),row.getAfterColumnsList());
                                    }catch (Exception e){
                                        log.error("新增处理发送异常！",e);
                                    }
                                });
                            });
                        }

                        //修改事件处理
                        if(eventType == CanalEntry.EventType.UPDATE){
                            change.getRowChange().getRowDatasList().forEach(row -> {
                                CanalListener.listenChangeEvent.get(key).getUpdateListeners().forEach(bean -> {
                                    try {
                                        bean.updateHandle(change.getHeader().getSchemaName(),change.getHeader().getTableName(),row.getBeforeColumnsList(),row.getAfterColumnsList());
                                    }catch (Exception e){
                                        log.error("修改处理发送异常！",e);
                                    }
                                });
                            });
                        }

                        //删除事件处理
                        if(eventType == CanalEntry.EventType.DELETE){
                            change.getRowChange().getRowDatasList().forEach(row -> {
                                CanalListener.listenChangeEvent.get(key).getDeleteListeners().forEach(bean -> {
                                    try {
                                        bean.deleteHandle(change.getHeader().getSchemaName(),change.getHeader().getTableName(),row.getBeforeColumnsList());
                                    }catch (Exception e){
                                        log.error("删除处理发送异常！",e);
                                    }
                                });
                            });
                        }
                    }
                });
            });
        }catch (Exception e){
            log.error("canal消息订阅处理发生异常！",e);
            return false;
        }


        return true;
    }

    /**
     * 数据修改操作，按事务分组
     * @param message 消息
     * @return 事务列表
     */
    private List<TransEvent> getTransactionList(Message message){
        //事务列表
        val transList = new ArrayList<TransEvent>();
        message.getEntries().forEach(entry -> {
            try {

                /** 事务开始 **/
                if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN){
                    //处理延迟时间（ms）
                    val delayTime = new Date().getTime() - entry.getHeader().getExecuteTime();
                    log.info("处理延迟时间 {}ms",delayTime);

                    val begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());

                    //记录事务开始
                    if(transList.size()==0){
                        transList.add(new TransEvent());
                    }
                    transList.get(transList.size()-1).setTransactionBegin(begin);
                }

                /** 数据变更 **/
                if(entry.getEntryType() == CanalEntry.EntryType.ROWDATA){
                    //解析数据变更信息
                    val rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());

                    //事件类型
                    val eventType = rowChange.getEventType();

                    //数据修改命令（非查询命令、非表结构修改命令）
                    if(eventType != CanalEntry.EventType.QUERY && !rowChange.getIsDdl()){
                        log.info("SQL：{}",rowChange.getSql());

                        //记录数据变更
                        transList.get(transList.size()-1).getRowChanges().add(new RowChangeInfo(entry.getHeader(),rowChange));

                    }
                }

                /** 事务结束 **/
                if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND){
                    val end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());

                    //记录事务结束
                    if(transList.size()!=0){
                        val curTrans = transList.get(transList.size()-1);
                        curTrans.setTransactionEnd(end);
                        curTrans.setTransactionId(end.getTransactionId());
                        transList.add(new TransEvent());
                    }
                }
            }catch (InvalidProtocolBufferException e){
                log.error("ProtocolBuffer 解码异常！",e);
                throw new RuntimeException(e);
            }

        });
        return transList;
    }
}
