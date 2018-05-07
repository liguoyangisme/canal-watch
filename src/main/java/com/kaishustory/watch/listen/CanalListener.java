package com.kaishustory.watch.listen;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.kaishustory.watch.common.BeanFactory;
import com.kaishustory.watch.listen.annotation.TableListener;
import com.kaishustory.watch.listen.interfaces.DeleteListener;
import com.kaishustory.watch.listen.interfaces.ICanalMessageHandle;
import com.kaishustory.watch.listen.interfaces.InsertListener;
import com.kaishustory.watch.listen.interfaces.UpdateListener;
import com.kaishustory.watch.listen.model.ListenBeans;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * 启动入口
 *
 * @author liguoyang
 * @create 2018-05-06 下午12:14
 **/
@Slf4j
@Component
public class CanalListener implements ApplicationRunner {

    /**
     * canal 服务名称
     */
    private String destination = "example";
    /**
     * canal 服务地址
     */
    private String ip = "127.0.0.1";
    /**
     * canal 服务端口
     */
    private int port = 11111;
    /**
     * canal 服务登录名
     */
    private String user = "";
    /**
     * canal 服务登录密码
     */
    private String password = "";

    /**
     * 循环订阅标志
     */
    private boolean running = true;

    /**
     * 表数据变更监听列表 Map<库.表, 监听列表>
     */
    public static Map<String,ListenBeans> listenChangeEvent;

    /**
     * Canal监听
     * @param args
     * @throws Exception
     */
    @Override
    public void run(ApplicationArguments args) throws Exception {

        //获取表数据变更监听列表
        listenChangeEvent = getListenChangeEvent();

        //连接Canal
        final val conn = CanalConnectors.newSingleConnector(new InetSocketAddress(ip,port),destination,user,password);

        final Thread thread = new Thread(()->{
            log.info("canal 订阅开始");
            //订阅消息
            subscribe(conn, new CanalMessageHandle());
        });

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            log.info("canal 订阅停止");
            stop(thread);
        }));

        thread.start();
    }

    /**
     * 数据变更消息订阅
     * @param conn Canal连接
     * @param canalMessageHandle 数据变更消息处理方法
     */
    @SneakyThrows
    private void subscribe(CanalConnector conn, ICanalMessageHandle canalMessageHandle){

        while (running) {
            try {
                //连接 canal
                conn.connect();
                //订阅数据变更
                conn.subscribe();

                while (running) {
                    try {
                        //读取数据变更消息
                        val message = conn.getWithoutAck(1000);
                        //变更数量
                        int size = message.getEntries().size();
                        //批处理ID
                        long batchId = message.getId();

                        //判断是否有可处理消息
                        if (batchId == -1 || size == 0) {
                            //无更新消息，
                            Thread.sleep(100);
                        } else {
                            //处理数据变更消息
                            val ack = canalMessageHandle.handle(message);
                            if (ack) {
                                //确认处理成功
                                conn.ack(batchId);
                            } else {
                                //处理失败，回滚数据
                                conn.rollback(batchId);
                            }
                        }
                    } catch (Exception e) {
                        log.error("canal 消息订阅异常！", e);

                        //尝试重连
                        try {
                            conn.disconnect();
                            Thread.sleep(100);
                            conn.connect();
                            log.info("canal 重连");
                        } catch (Exception cce) {
                            log.error("canal 重连失败！", cce);
                        }
                    }

                }
            }catch (CanalClientException e){
                log.error("canal 连接失败！",e);
                Thread.sleep(1000);
            }
        }
    }

    /**
     * 停止订阅
     * @param thread 线程
     */
    @SneakyThrows
    private void stop(Thread thread) {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            thread.join();
        }
        Thread.sleep(1000);
    }

    /**
     * 表数据变更监听列表
     * @return Map<库.表, 监听列表>
     */
    private Map<String,ListenBeans> getListenChangeEvent(){
        //搜索定义数据变更监听的Bean
        val beanNames = BeanFactory.getApplicationContext().getBeanNamesForAnnotation(TableListener.class);

        //监听表关系 Map<数据库.表, List<监听处理对象>>
        val canal_listen_map = new HashMap<String,ListenBeans>();

        for(String beanName : beanNames){
            //监听处理对象
            Object bean = BeanFactory.getBean(beanName);
            //映射key
            TableListener tableListener = bean.getClass().getAnnotation(TableListener.class);
            //key = 数据库.表名
            String key = tableListener.database()+"."+tableListener.table();
            //记录监听Bean
            if(!canal_listen_map.containsKey(key)){
                canal_listen_map.put(key, new ListenBeans());
            }

            //记录数据新增监听事件
            if(bean instanceof InsertListener){
                val insert = (InsertListener)bean;
                canal_listen_map.get(key).getInsertListeners().add(insert);
            }
            //记录数据修改监听事件
            if(bean instanceof UpdateListener){
                val update = (UpdateListener)bean;
                canal_listen_map.get(key).getUpdateListeners().add(update);
            }
            //记录数据删除监听事件
            if(bean instanceof DeleteListener){
                val delete = (DeleteListener)bean;
                canal_listen_map.get(key).getDeleteListeners().add(delete);
            }
        }
        return canal_listen_map;
    }
}
