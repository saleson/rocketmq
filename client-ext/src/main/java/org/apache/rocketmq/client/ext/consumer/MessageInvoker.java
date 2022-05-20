package org.apache.rocketmq.client.ext.consumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author saleson
 * @date 2022-05-19 09:06
 */
public interface MessageInvoker {


    ConsumeHandleResult consumeMessage(final List<MessageExt> msgs,
                                       final ConsumeHandleContext context);


    class MessageListenerOrderlyInvoker implements MessageInvoker {

        private final MessageListenerOrderly messageListener;

        public MessageListenerOrderlyInvoker(MessageListenerOrderly messageListener) {
            this.messageListener = messageListener;
        }

        @Override
        public ConsumeHandleResult consumeMessage(List<MessageExt> msgs, ConsumeHandleContext context) {
            ConsumeOrderlyStatus consumeStatus = messageListener.consumeMessage(msgs, context.getConsumeOrderlyContext());
            return new ConsumeHandleResult(ConsumeType.ORDERLY, consumeStatus);
        }
    }

    class MessageListenerConcurrentlyInvoker implements MessageInvoker{

        private final MessageListenerConcurrently messageListener;

        public MessageListenerConcurrentlyInvoker(MessageListenerConcurrently messageListener) {
            this.messageListener = messageListener;
        }

        @Override
        public ConsumeHandleResult consumeMessage(List<MessageExt> msgs, ConsumeHandleContext context) {
            ConsumeConcurrentlyStatus consumeStatus = messageListener.consumeMessage(msgs, context.getConsumeConcurrentlyContext());
            return new ConsumeHandleResult(ConsumeType.CONCURRENTLY, consumeStatus);
        }
    }

}
