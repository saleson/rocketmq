package org.apache.rocketmq.client.ext.consumer;

import org.apache.rocketmq.client.consumer.MQConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author saleson
 * @date 2022-05-18 17:06
 */
public class MQConsumerDelegater {
    private String consumerGroup;

    private GrayState grayState = GrayState.Unknow;

    private MQConsumer mqConsumer;
    private MQConsumerManager mqConsumerManager = MQConsumerManager.INSTANCE;


//    private Map<String, SubscriptionInfo> subscriptionInfoMap;

    private Map<String, SubscriptionData> oriSubscriptDatas;
    private Map<String, MessageSelector> oriMessageSelectors;

    public MQConsumerDelegater(String consumerGroup, MQConsumer mqConsumer) {
        this.consumerGroup = consumerGroup;
        this.mqConsumer = mqConsumer;
//        subscriptionInfoMap = new ConcurrentHashMap<>();
        oriSubscriptDatas = new ConcurrentHashMap<>();
        oriMessageSelectors = new ConcurrentHashMap<>();
    }

    public MQConsumer getMQConsumer() {
        return mqConsumer;
    }

    public Set<String> tipics() {
//        return subscriptionInfoMap.keySet();
        return oriSubscriptDatas.keySet();
    }

    public Map<String, SubscriptionData> getOriSubscriptDatas(){
        return oriSubscriptDatas;
    }

    public SubscriptionData getOriSubscriptionData(String topic){
        return oriSubscriptDatas.get(topic);
    }

    public void putSubscriptionData(SubscriptionData subscriptionData){
        oriSubscriptDatas.put(subscriptionData.getTopic(), subscriptionData);
        mqConsumerManager.putMQConsumerDelegater(subscriptionData.getTopic(), this);
    }

    public void removeOriSubscriptionData(String topic){
        oriSubscriptDatas.remove(topic);
        mqConsumerManager.removeMQConsumerDelegater(topic, getConsumerGroup());
    }

    public Map<String, MessageSelector> getOriMessageSelectors() {
        return oriMessageSelectors;
    }

    public MessageSelector getOriMessageSelector(String topic){
        return oriMessageSelectors.get(topic);
    }

    public void removeOriMessageSelector(String topic){
        oriMessageSelectors.remove(topic);
    }

    public void putOriMessageSelector(String topic, MessageSelector messageSelector){
        oriMessageSelectors.put(topic, messageSelector);
    }

    public void putOriMessageSelector(String topic, String subExpression){
        oriMessageSelectors.put(topic, MessageSelector.byTag(subExpression));
    }

    //    public Map<String, SubscriptionInfo> getSubscriptionInfos() {
//        return subscriptionInfoMap;
//    }
//
//    public void putSubscriptionInfo(SubscriptionInfo subscriptionInfo) {
//        subscriptionInfoMap.put(subscriptionInfo.getTopic(), subscriptionInfo);
//    }
//
//    public SubscriptionInfo removeSubscriptionInfo(String topic) {
//        return subscriptionInfoMap.remove(topic);
//    }

    public void setGrayState(GrayState grayState) {
        this.grayState = grayState;
    }


    public String getConsumerGroup() {
        return consumerGroup;
    }

    public GrayState getGrayState() {
        return grayState;
    }

    public enum GrayState{
        Unknow, YES, NO;
    }




}
