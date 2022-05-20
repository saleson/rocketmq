package org.apache.rocketmq.client.ext.consumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author saleson
 * @date 2022-05-19 09:08
 */
public interface MessageListenHandler {

    ConsumeHandleResult consumeMessage(MessageInvoker messageInvoker,
                                        final List<MessageExt> msgs,
                                        final ConsumeHandleContext context);

    
}
