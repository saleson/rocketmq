package org.apache.rocketmq.client.ext.consumer;

import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author saleson
 * @date 2022-05-20 10:53
 */
public class MessageInvokerChainBuilder {


    public static MessageListenerOrderly buildProxyMessageListenerOrderly(MessageListenerOrderly original) {
        final MessageInvoker messageInvoker = buildOrderlyChaninInvoker(original);
        return new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                ConsumeHandleContext chCxt = new ConsumeHandleContext(ConsumeType.ORDERLY, context);
                ConsumeHandleResult result = messageInvoker.consumeMessage(msgs, chCxt);
                return result.getConsumeOrderlyStatus();
            }
        };
    }

    public static MessageListenerConcurrently buildProxyMessageListenerConcurrently(MessageListenerConcurrently original) {
        final MessageInvoker messageInvoker = buildConcurrentlyChainInvoker(original);
        return new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                ConsumeHandleContext chCxt = new ConsumeHandleContext(ConsumeType.CONCURRENTLY, context);
                ConsumeHandleResult result = messageInvoker.consumeMessage(msgs, chCxt);
                return result.getConsumeConcurrentlyStatus();
            }
        };
    }


    public static MessageInvoker buildOrderlyChaninInvoker(MessageListenerOrderly listenerOrderly) {
        MessageInvoker originalInvoker = new MessageInvoker.MessageListenerOrderlyInvoker(listenerOrderly);
        return buildChainInvoker(originalInvoker);
    }

    public static MessageInvoker buildConcurrentlyChainInvoker(MessageListenerConcurrently listenerConcurrently) {
        MessageInvoker originalInvoker = new MessageInvoker.MessageListenerConcurrentlyInvoker(listenerConcurrently);
        return buildChainInvoker(originalInvoker);
    }

    private static MessageInvoker buildChainInvoker(MessageInvoker originalInvoker) {
        List<MessageListenHandler> handlers = getMessageListenHandlers();
        MessageInvoker next = originalInvoker;
        for (int i = handlers.size(); i > 0; i--) {
            MessageListenHandler listenHandler = handlers.get(i-1);
            next = new MessageInvokerChainNode(listenHandler, next);
        }
        return next;
    }


    private static List<MessageListenHandler> staticMessageListenHandlers = Collections.emptyList();

    private static List<MessageListenHandler> getMessageListenHandlers() {
//        List<MessageListenHandler> handlers = new ArrayList<>();
//        //todo
//        handlers.add(new LogMessageListenHandler());
//        return handlers;
        return staticMessageListenHandlers;
    }

    public static synchronized void addStaticMessageListenHandler(MessageListenHandler messageListenHandler){
        List<MessageListenHandler> staticMessageListenHandlers = new ArrayList<>(MessageInvokerChainBuilder.staticMessageListenHandlers);
        staticMessageListenHandlers.add(messageListenHandler);
        MessageInvokerChainBuilder.staticMessageListenHandlers = staticMessageListenHandlers;
    }

    private static class MessageInvokerChainNode implements MessageInvoker {

        private final MessageListenHandler listenHandler;
        private final MessageInvoker next;

        public MessageInvokerChainNode(MessageListenHandler listenHandler, MessageInvoker next) {
            this.listenHandler = listenHandler;
            this.next = next;
        }

        @Override
        public ConsumeHandleResult consumeMessage(List<MessageExt> msgs, ConsumeHandleContext context) {
            return listenHandler.consumeMessage(next, msgs, context);
        }
    }


//    private static class LogMessageListenHandler implements MessageListenHandler {
//
//        @Override
//        public ConsumeHandleResult consumeMessage(MessageInvoker messageInvoker, List<MessageExt> msgs, ConsumeHandleContext context) {
//            System.out.println(String.format("LogMessageListenHandler.consumeMessage start, consumeType:%s,  msg:%s",
//                    context.getConsumeType().toString(), msgs.toString()));
//            ConsumeHandleResult result = messageInvoker.consumeMessage(msgs, context);
//            System.out.println(String.format("LogMessageListenHandler.consumeMessage end, status:%s",
//                    result.getOriginalConsumeStatus().toString()));
//            return result;
//        }
//    }

}
