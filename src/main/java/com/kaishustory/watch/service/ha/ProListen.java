package com.kaishustory.watch.service.ha;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.kaishustory.watch.listen.annotation.TableListener;
import com.kaishustory.watch.listen.interfaces.DeleteListener;
import com.kaishustory.watch.listen.interfaces.InsertListener;
import com.kaishustory.watch.listen.interfaces.UpdateListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 数据变更事件处理
 * @author liguoyang
 * @create 2018-05-06 下午5:24
 **/
@Slf4j
@Service
@TableListener(database = "ha",table = "pro")
public class ProListen implements InsertListener,UpdateListener,DeleteListener{

    /**
     * 新增事件处理
     * @param database 数据库
     * @param table 表名
     * @param columns 列
     * @return
     */
    @Override
    public boolean insertHandle(String database, String table, List<CanalEntry.Column> columns) {
        log.info("新增事件");
        return true;
    }

    /**
     * 修改事件处理
     * @param database 数据库
     * @param table 表名
     * @param beforeColumns 修改前列
     * @param afterColumns 修改后列
     * @return
     */
    @Override
    public boolean updateHandle(String database, String table, List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns) {
        log.info("修改事件");
        return true;
    }

    /**
     * 删除事件处理å
     * @param database 数据库
     * @param table 表名
     * @param columns 列
     * @return
     */
    @Override
    public boolean deleteHandle(String database, String table, List<CanalEntry.Column> columns) {
        log.info("删除事件");
        return true;
    }

}
