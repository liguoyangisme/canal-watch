package com.kaishustory.watch.listen.model;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 事务对象
 * @author liguoyang
 * @create 2018-05-06 下午4:04
 **/
@Data
public class TransEvent {

    /**
     * 事务ID
     */
    private String transactionId;

    /**
     * 事务开始
     */
    private CanalEntry.TransactionBegin transactionBegin;

    /**
     * 数据变更
     */
    private List<RowChangeInfo> rowChanges = new ArrayList<>();

    /**
     * 事务结束
     */
    private CanalEntry.TransactionEnd transactionEnd;

    /**
     * 是否有变更
     */
    public boolean hasChange(){
        return transactionBegin!=null && rowChanges.size()>0 && transactionEnd!=null;
    }
}
