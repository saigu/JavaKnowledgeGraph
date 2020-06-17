package com.alibaba.otter.canal.instance.core;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;

/**
 * 代表单个canal实例，比如一个destination会独立一个实例
 * 
 * @author jianghang 2012-7-12 下午12:04:58
 * @version 1.0.0
 */
public interface CanalInstance extends CanalLifeCycle {

    /**
     * note:
     * instance对应的destination
     * @return
     */
    String getDestination();

    /**
     * note:
     * 数据源接入，模拟slave协议和master进行交互，协议解析，位于canal.parse模块中
     * @return
     */
    CanalEventParser getEventParser();

    /**
     * note:
     * Parser和Store链接器，进行数据过滤，加工，分发的工作
     * @return
     */
    CanalEventSink getEventSink();

    /**
     * note:
     * 数据存储
     * @return
     */
    CanalEventStore getEventStore();

    /**
     * note:
     * 增量订阅 & 消费信息管理器
     * @return
     */
    CanalMetaManager getMetaManager();

    /**
     * 告警，位于canal.common块中
     * @return
     */
    CanalAlarmHandler getAlarmHandler();

    /**
     * 客户端发生订阅/取消订阅行为
     */
    boolean subscribeChange(ClientIdentity identity);

    /**
     * note:
     * 相关mq的配置信息，用作mqProducer的入参
     * @return
     */
    CanalMQConfig getMqConfig();
}
