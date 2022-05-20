package org.apache.rocketmq.client.ext.consumer.impl;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.ext.consumer.MQConsumerDelegater;
import org.apache.rocketmq.client.ext.consumer.MessageInvokerChainBuilder;
import org.apache.rocketmq.client.ext.consumer.SubscriptionDataHelper;
import org.apache.rocketmq.client.ext.gray.GrayManager;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.Slf4jLoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * @author saleson
 * @date 2022-05-18 15:12
 */
public class ExtMQPUshConsumer extends DefaultMQPushConsumer {

    private static InternalLogger logger = Slf4jLoggerFactory.getLogger(ExtMQPUshConsumer.class);

    private MQConsumerDelegater mqConsumerDelegater;
    private GrayManager grayManager = GrayManager.INSTANCE;

    public ExtMQPUshConsumer(String consumerGroup) {
        super(consumerGroup);
        init();
    }


    public void setGrayManager(GrayManager grayManager) {
        this.grayManager = grayManager;
    }

    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        super.subscribe(topic, subExpression);
        enrolOriSubscriptionData(topic);
    }

    @Override
    public void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
        super.subscribe(topic, messageSelector);
        enrolOriSubscriptionData(topic);
    }

    @Override
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        super.subscribe(topic, fullClassName, filterClassSource);
        enrolOriSubscriptionData(topic);
    }

    @Override
    public void unsubscribe(String topic) {
        super.unsubscribe(topic);
        unenrolOriSubscriptionData(topic);
    }


    private void init() {
        mqConsumerDelegater = new MQConsumerDelegater(getConsumerGroup(), this);
    }

    private void unenrolOriSubscriptionData(String topic) {
        mqConsumerDelegater.removeOriSubscriptionData(topic);
    }

    private void enrolOriSubscriptionData(String topic) {
        SubscriptionData subscriptionData = defaultMQPushConsumerImpl.getSubscriptionInner().get(topic);
        mqConsumerDelegater.putSubscriptionData(subscriptionData);
    }

    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        MessageListenerOrderly proxy = MessageInvokerChainBuilder.buildProxyMessageListenerOrderly(messageListener);
        super.registerMessageListener(proxy);
    }

    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        MessageListenerConcurrently proxy = MessageInvokerChainBuilder.buildProxyMessageListenerConcurrently(messageListener);
        super.registerMessageListener(proxy);
    }

    @Override
    public void registerMessageListener(MessageListener messageListener) {
        MessageListener delegate = messageListener;
        if (messageListener instanceof MessageListenerOrderly) {
            delegate = MessageInvokerChainBuilder.buildProxyMessageListenerOrderly((MessageListenerOrderly) messageListener);
        } else if (messageListener instanceof MessageListenerConcurrently) {
            delegate = MessageInvokerChainBuilder.buildProxyMessageListenerConcurrently((MessageListenerConcurrently) messageListener);
        }
        super.registerMessageListener(delegate);
    }

    @Override
    public void start() throws MQClientException {
//        if(Objects.isNull(mqConsumerDelegater)){
//            mqConsumerDelegater = new MQConsumerDelegater(getConsumerGroup(),this);
//        }
//        Set<String> topics = mqConsumerDelegater.tipics();
//        ConcurrentMap<String, SubscriptionData> subscriptionDatas = defaultMQPushConsumerImpl.getSubscriptionInner();
//        subscriptionDatas.forEach((topic, subData)->{
//            if(!topics.contains(topic)){
//                SubscriptionInfo subscriptionInfo = new SubscriptionInfo(topic);
//                subscriptionInfo.setSubscriptionData(subData);
//                mqConsumerDelegater.putSubscriptionInfo();
//            }
//        });

        boolean onlyGray = grayManager.isOnlyAccessGrayMessage();
        if (!onlyGray && mqConsumerDelegater.getGrayState() == MQConsumerDelegater.GrayState.YES) {
            mqConsumerDelegater.setGrayState(MQConsumerDelegater.GrayState.NO);
            ConcurrentMap<String, SubscriptionData> subscriptionDatas = defaultMQPushConsumerImpl.getSubscriptionInner();
            for (String topic : subscriptionDatas.keySet()) {
                SubscriptionData sdata = mqConsumerDelegater.getOriSubscriptionData(topic);
                if (Objects.nonNull(sdata)) {
                    subscriptionDatas.put(topic, sdata);
                } else {
                    logger.warn("[recovery normal read] not found original SubscriptionData from mqConsumerDelegater, the topic is '{}', subscription data is :{}",
                            topic, subscriptionDatas.get(topic));
                }
            }
            setConsumerGroup(mqConsumerDelegater.getConsumerGroup());
        } else if (onlyGray) {
            mqConsumerDelegater.setGrayState(MQConsumerDelegater.GrayState.YES);
            ConcurrentMap<String, SubscriptionData> subscriptionDatas = defaultMQPushConsumerImpl.getSubscriptionInner();
            for (Map.Entry<String, SubscriptionData> entry : subscriptionDatas.entrySet()) {
                SubscriptionData sdata = mqConsumerDelegater.getOriSubscriptionData(entry.getKey());
                if (Objects.isNull(sdata)) {
                    logger.info("[read gray] not found original SubscriptionData from mqConsumerDelegater, the topic is '{}', subscription data is :{}",
                            entry.getKey(), entry.getValue());
                    sdata = entry.getValue();
                    mqConsumerDelegater.putSubscriptionData(sdata);
                    continue;
                } else if (entry.getValue().isClassFilterMode()) {
                    logger.warn("[read gray] SubscriptionData is class filter mode , the topic is '{}', subscription data is :{}",
                            entry.getKey(), entry.getValue());
                    continue;
                }
                SubscriptionData newsdata = SubscriptionDataHelper.transformToIncludeGray(grayManager.getGrayTag(), sdata);
                subscriptionDatas.put(newsdata.getTopic(), newsdata);
                setConsumerGroup(mqConsumerDelegater.getConsumerGroup() + "_" + grayManager.getGrayTag());
            }
        }
        super.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }
}
