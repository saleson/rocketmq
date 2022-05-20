package org.apache.rocketmq.client.ext.producer;

import org.apache.rocketmq.common.message.Message;

import java.util.List;

/**
 * @author saleson
 * @date 2022-05-18 15:40
 */
public class MultiMessageTransformer implements MessageTransformer {
    private List<MessageTransformer> messageTransformers;


    public MultiMessageTransformer(List<MessageTransformer> messageTransformers) {
        this.messageTransformers = messageTransformers;
    }

    @Override
    public Message transform(Message message) {
        Message msg = message;
        for (MessageTransformer messageTransformer : messageTransformers) {
            msg = messageTransformer.transform(msg);
        }
        return msg;
    }
}
