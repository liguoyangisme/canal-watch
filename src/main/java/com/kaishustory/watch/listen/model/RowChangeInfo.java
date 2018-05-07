package com.kaishustory.watch.listen.model;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;

/**
 * 数据变更信息
 *
 * @author liguoyang
 * @create 2018-05-06 下午5:42
 **/
@Data
public class RowChangeInfo {

    public RowChangeInfo() {
    }

    public RowChangeInfo(CanalEntry.Header header, CanalEntry.RowChange rowChange) {
        this.header = header;
        this.rowChange = rowChange;
    }

    /**
     * 基本信息
     */
    private CanalEntry.Header header;

    /**
     * 变更信息
     */
    private CanalEntry.RowChange rowChange;
}
