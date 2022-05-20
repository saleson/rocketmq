package org.apache.rocketmq.client.ext.producer.impl;

import org.apache.rocketmq.client.ext.producer.ExtProducerHelper;
import org.apache.rocketmq.client.ext.producer.MessageTransformer;
import org.apache.rocketmq.client.ext.producer.MultiMessageTransformer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * @author saleson
 * @date 2022-05-18 15:25
 */
public class ExtMQProducerBuilders {


    public static DefaultMQProducer defaultMQProducer(String producerGroup) {
        return new ExtMQProducer(producerGroup, ExtProducerHelper.multiSPIMessageTransformer());
    }




}
