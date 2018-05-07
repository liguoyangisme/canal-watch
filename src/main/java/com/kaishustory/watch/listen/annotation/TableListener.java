package com.kaishustory.watch.listen.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 数据表监听注解
 *
 * @author liguoyang
 * @create 2018-05-06 下午5:26
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TableListener {

    /**
     * 数据库
     */
    String database();

    /**
     * 表
     */
    String table();


}
