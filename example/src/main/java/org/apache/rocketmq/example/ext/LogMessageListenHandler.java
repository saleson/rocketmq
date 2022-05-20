package org.apache.rocketmq.example.ext;

import org.apache.rocketmq.client.ext.consumer.ConsumeHandleContext;
import org.apache.rocketmq.client.ext.consumer.ConsumeHandleResult;
import org.apache.rocketmq.client.ext.consumer.MessageInvoker;
import org.apache.rocketmq.client.ext.consumer.MessageListenHandler;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author saleson
 * @date 2022-05-20 14:08
 */
public class LogMessageListenHandler implements MessageListenHandler {

    @Override
    public ConsumeHandleResult consumeMessage(MessageInvoker messageInvoker, List<MessageExt> msgs, ConsumeHandleContext context) {
        System.out.println(String.format("\n\nhas %d messages", msgs.size()));

//        System.out.println(String.format("LogMessageListenHandler.consumeMessage start, consumeType:%s,  msg:%s",
//                context.getConsumeType().toString(), msgs.toString()));
        ConsumeHandleResult result = messageInvoker.consumeMessage(msgs, context);
//        System.out.println(String.format("LogMessageListenHandler.consumeMessage end, status:%s",
//                result.getOriginalConsumeStatus().toString()));

        return result;
    }
}