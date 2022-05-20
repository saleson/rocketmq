package org.apache.rocketmq.client.ext.consumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;

import java.util.Objects;

/**
 * @author saleson
 * @date 2022-05-19 11:21
 */
public class ConsumeHandleResult {
    private final ConsumeType consumeType;
    private final Object originalConsumeStatus;

    public ConsumeHandleResult(ConsumeType consumeType, Object originalConsumeStatus) {
        this.consumeType = consumeType;
        this.originalConsumeStatus = originalConsumeStatus;
    }


    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public Object getOriginalConsumeStatus() {
        return originalConsumeStatus;
    }


    public ConsumeOrderlyStatus getConsumeOrderlyStatus() {
        return Objects.equals(consumeType, ConsumeType.ORDERLY) ?
                (ConsumeOrderlyStatus) originalConsumeStatus : null;
    }

    public ConsumeConcurrentlyStatus getConsumeConcurrentlyStatus() {
        return Objects.equals(consumeType, ConsumeType.CONCURRENTLY) ?
                (ConsumeConcurrentlyStatus) originalConsumeStatus : null;
    }
}
