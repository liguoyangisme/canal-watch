package com.kaishustory.watch.listen.interfaces;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

/**
 * 监听删除事件
 *
 * @author liguoyang
 * @create 2018-05-06 下午5:50
 **/
public interface DeleteListener {

    /**
     * 删除数据处理
     * @param database 数据库
     * @param table 表名
     * @param columns 列
     * @return 处理成功
     */
    boolean deleteHandle(String database, String table, List<CanalEntry.Column> columns);
}
