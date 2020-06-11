package com.alibaba.otter.canal.server;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;

import java.util.concurrent.TimeUnit;

public interface CanalService {

    /**
     * note：
     * 订阅
     * @param clientIdentity
     * @throws CanalServerException
     */
    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;

    /**
     * note:
     * 取消订阅
     * @param clientIdentity
     * @throws CanalServerException
     */
    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;

    /**
     * note:
     * 批量获取message，并自动ACK
     * @param clientIdentity
     * @param batchSize
     * @return
     * @throws CanalServerException
     */
    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    /**
     * note:
     * 在超时时间内，批量获取message，并自动ack
     * @param clientIdentity
     * @param batchSize
     * @param timeout
     * @param unit
     * @return
     * @throws CanalServerException
     */
    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
            throws CanalServerException;

    /**
     * note:
     * 批量获取message，不ACK
     * @param clientIdentity
     * @param batchSize
     * @return
     * @throws CanalServerException
     */
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    /**
     * note:
     * 在超时时间内，批量获取message，不ack
     * @param clientIdentity
     * @param batchSize
     * @param timeout
     * @param unit
     * @return
     * @throws CanalServerException
     */
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
            throws CanalServerException;

    /**
     * note:
     * ack某个批次的数据
     * @param clientIdentity
     * @param batchId
     * @throws CanalServerException
     */
    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;

    /**
     * note:
     * 回滚所有没有ack的批次数据
     * @param clientIdentity
     * @throws CanalServerException
     */
    void rollback(ClientIdentity clientIdentity) throws CanalServerException;

    /**
     * note:
     * 回滚某个批次的数据
     * @param clientIdentity
     * @param batchId
     * @throws CanalServerException
     */
    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
