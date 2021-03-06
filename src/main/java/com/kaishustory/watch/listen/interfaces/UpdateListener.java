package com.kaishustory.watch.listen.interfaces;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

/**
 * 监听修改事件
 *
 * @author liguoyang
 * @create 2018-05-06 下午5:50
 **/
public interface UpdateListener {

    /**
     * 修改数据处理
     * @param database 数据库
     * @param table 表名
     * @param beforeColumns 修改前列
     * @param afterColumns 修改后列
     * @return 处理成功
     */
    boolean updateHandle(String database, String table, List<CanalEntry.Column> beforeColumns,List<CanalEntry.Column> afterColumns);
}
