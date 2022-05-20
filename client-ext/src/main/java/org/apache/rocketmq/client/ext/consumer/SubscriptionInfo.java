package org.apache.rocketmq.client.ext.consumer;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * @author saleson
 * @date 2022-05-18 17:26
 */
public class SubscriptionInfo {
    private final String topic;
    private boolean usedGray;
//    private SubscriptOriginInfo originInfo;
    private SubscriptionData subscriptionData;

    public SubscriptionInfo(String topic) {
        this.topic = topic;
    }


    public String getTopic() {
        return topic;
    }

    public boolean isUsedGray() {
        return usedGray;
    }

    public void setUsedGray(boolean usedGray) {
        this.usedGray = usedGray;
    }

//    public SubscriptOriginInfo getOriginInfo() {
//        return originInfo;
//    }
//
//    public void setOriginInfo(SubscriptOriginInfo originInfo) {
//        this.originInfo = originInfo;
//    }

    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public void setSubscriptionData(SubscriptionData subscriptionData) {
        this.subscriptionData = subscriptionData;
    }
}
