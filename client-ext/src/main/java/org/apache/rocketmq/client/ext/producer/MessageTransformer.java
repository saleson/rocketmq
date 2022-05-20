package org.apache.rocketmq.client.ext.producer;

import org.apache.rocketmq.common.message.Message;

/**
 * @author saleson
 * @date 2022-05-18 15:30
 */
public interface MessageTransformer {

    Message transform(Message message);

}
