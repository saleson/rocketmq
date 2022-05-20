/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.ext;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.ext.consumer.*;
import org.apache.rocketmq.client.ext.consumer.impl.ExtMQPUshConsumer;
import org.apache.rocketmq.client.ext.gray.GrayManager;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class NormalConsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {


        GrayManager grayManager = new GrayManager(false, "gray");
        MessageInvokerChainBuilder.addStaticMessageListenHandler(new LogMessageListenHandler());
        MessageInvokerChainBuilder.addStaticMessageListenHandler(new MessageListenHandler() {

            @Override
            public ConsumeHandleResult consumeMessage(MessageInvoker messageInvoker, List<MessageExt> msgs, ConsumeHandleContext context) {
                List<MessageExt> nmsgs = msgs.stream().filter(msg -> {
                    String routingTag = msg.getUserProperty(GrayManager.MSG_ROUTING_TAG_KEY);
                    boolean r = !Objects.equals(routingTag, grayManager.getGrayTag());
                    if (!r) {
                        System.out.println(String.format("== skip msg, id:%s, routingTag:%s, msg.properties:%s, msg.body: %s",
                                msg.getMsgId(), routingTag, msg.getProperties(), new String(msg.getBody())));
                    }
                    return r;
                }).collect(Collectors.toList());

                return messageInvoker.consumeMessage(nmsgs, context);
            }
        });


        /*
         * Instantiate with specified consumer group name.
         */
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");
        ExtMQPUshConsumer consumer = new ExtMQPUshConsumer("please_rename_unique_group_name_4");
        consumer.setGrayManager(grayManager);
        consumer.setNamesrvAddr("localhost:9876");

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */

        /*
         * Specify where to start in case the specific consumer group is a brand-new one.
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*
         * Subscribe one more topic to consume.
         */
//        consumer.subscribe("TopicTest", "*");
//        consumer.subscribe("TopicTest", MessageSelector.bySql("TAGS in ('tf')"));
//        consumer.subscribe("TopicTest", MessageSelector.bySql("TAGS='tf'"));
//        consumer.subscribe("TopicTest", MessageSelector.bySql("TAGS=tf"));

        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {

//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                msgs.forEach(msg ->{
                    System.out.printf("msg.properties: %s, msg.content: %s %n", msg.getProperties().toString(), new String(msg.getBody()));

                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
