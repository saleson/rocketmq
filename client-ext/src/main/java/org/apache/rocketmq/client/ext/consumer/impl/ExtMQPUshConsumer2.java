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
import org.apache.rocketmq.common.filter.FilterAPI;
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
public class ExtMQPUshConsumer2 extends DefaultMQPushConsumer {

    private static InternalLogger logger = Slf4jLoggerFactory.getLogger(ExtMQPUshConsumer2.class);

    private MQConsumerDelegater mqConsumerDelegater;

    private GrayManager grayManager = GrayManager.INSTANCE;

    public ExtMQPUshConsumer2(String consumerGroup) {
        super(consumerGroup);
        init();
    }

    public void setGrayManager(GrayManager grayManager) {
        this.grayManager = grayManager;
    }

    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        super.subscribe(topic, subExpression);
        mqConsumerDelegater.putOriMessageSelector(topic, subExpression);
    }

    @Override
    public void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
        super.subscribe(topic, messageSelector);
        mqConsumerDelegater.putOriMessageSelector(topic, messageSelector);
    }

    @Override
    public void unsubscribe(String topic) {
        super.unsubscribe(topic);
        mqConsumerDelegater.removeOriMessageSelector(topic);
    }

//    @Override
//    public void setMessageListener(MessageListener messageListener) {
//        super.setMessageListener(messageListener);
//    }

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


    private void init() {
        mqConsumerDelegater = new MQConsumerDelegater(getConsumerGroup(), this);
    }

    @Override
    public void start() throws MQClientException {
        boolean onlyGray = grayManager.isOnlyAccessGrayMessage();
        if (!onlyGray && mqConsumerDelegater.getGrayState() == MQConsumerDelegater.GrayState.YES) {
            try {
                mqConsumerDelegater.setGrayState(MQConsumerDelegater.GrayState.NO);
                ConcurrentMap<String, SubscriptionData> subscriptionDatas = defaultMQPushConsumerImpl.getSubscriptionInner();

                for (String topic : subscriptionDatas.keySet()) {
                    MessageSelector ms = mqConsumerDelegater.getOriMessageSelector(topic);
                    if (Objects.nonNull(ms)) {
                        SubscriptionData subscriptionData = FilterAPI.build(topic,
                                ms.getExpression(), ms.getExpressionType());
                        subscriptionDatas.put(topic, subscriptionData);

                    } else {
                        logger.warn("[recovery normal read] not found original SubscriptionData from mqConsumerDelegater, the topic is '{}', subscription data is :{}",
                                topic, subscriptionDatas.get(topic));
                    }
                }
            } catch (Exception e) {
                throw new MQClientException("subscription exception", e);
            }
            setConsumerGroup(mqConsumerDelegater.getConsumerGroup());
        } else if (onlyGray) {
            mqConsumerDelegater.setGrayState(MQConsumerDelegater.GrayState.YES);
            ConcurrentMap<String, SubscriptionData> subscriptionDatas = defaultMQPushConsumerImpl.getSubscriptionInner();
            for (Map.Entry<String, SubscriptionData> entry : subscriptionDatas.entrySet()) {
                MessageSelector ms = mqConsumerDelegater.getOriMessageSelector(entry.getKey());
                if (Objects.isNull(ms)) {
                    logger.info("[read gray] not found original SubscriptionData from mqConsumerDelegater, the topic is '{}', subscription data is :{}",
                            entry.getKey(), entry.getValue());
                    continue;
                }
                SubscriptionData newsdata = SubscriptionDataHelper.transformToIncludeGray(grayManager.getGrayTag(), entry.getKey(), ms);
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
