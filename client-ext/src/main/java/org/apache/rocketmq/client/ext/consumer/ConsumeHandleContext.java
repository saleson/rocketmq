package org.apache.rocketmq.client.ext.consumer;


import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;

import java.util.Objects;

/**
 * @author saleson
 * @date 2022-05-19 11:09
 */
public class ConsumeHandleContext {
    private final ConsumeType consumeType;
    private final Object originalConsumeContext;

    public ConsumeHandleContext(ConsumeType consumeType, Object originalConsumeContext) {
        this.consumeType = consumeType;
        this.originalConsumeContext = originalConsumeContext;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public Object getOriginalConsumeContext() {
        return originalConsumeContext;
    }

    public ConsumeOrderlyContext getConsumeOrderlyContext() {
        return Objects.equals(consumeType, ConsumeType.ORDERLY) ?
                (ConsumeOrderlyContext) originalConsumeContext : null;
    }

    public ConsumeConcurrentlyContext getConsumeConcurrentlyContext() {
        return Objects.equals(consumeType, ConsumeType.CONCURRENTLY) ?
                (ConsumeConcurrentlyContext) originalConsumeContext : null;
    }
}
