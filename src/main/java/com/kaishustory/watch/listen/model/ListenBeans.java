package com.kaishustory.watch.listen.model;

import com.kaishustory.watch.listen.interfaces.DeleteListener;
import com.kaishustory.watch.listen.interfaces.InsertListener;
import com.kaishustory.watch.listen.interfaces.UpdateListener;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Canal监听Bean集合
 *
 * @author liguoyang
 * @create 2018-05-06 下午6:22
 **/
@Value
public class ListenBeans {

    /**
     * 新增监听列表
     */
    private List<InsertListener> insertListeners = new ArrayList<>();

    /**
     * 修改监听列表
     */
    private List<UpdateListener> updateListeners = new ArrayList<>();

    /**
     * 删除监听列表
     */
    private List<DeleteListener> deleteListeners = new ArrayList<>();
}
