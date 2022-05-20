package org.apache.rocketmq.client.ext.consumer;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author saleson
 * @date 2022-05-18 16:23
 */
public class MQConsumerManager {

    public static MQConsumerManager INSTANCE = new MQConsumerManager();

    private Map<String, MQConsumerDelegater> mqConsumerDelegaters = new ConcurrentHashMap<>();

    public static String generateTopicConsumerKey(String topic, String consumerGroup) {
        return /*consumerGroup + "::" + */topic;
    }

    public MQConsumerDelegater getMQConsumerDelegater(String key) {
        return mqConsumerDelegaters.get(key);
    }

    public void removeMQConsumerDelegater(String topic, String consumerGroup) {
        removeMQConsumerDelegater(generateTopicConsumerKey(topic, consumerGroup));
    }

    public void removeMQConsumerDelegater(String key) {
        mqConsumerDelegaters.remove(key);
    }

    public void putMQConsumerDelegater(String topic, MQConsumerDelegater delegater) {
        mqConsumerDelegaters.put(generateTopicConsumerKey(topic, delegater.getConsumerGroup()), delegater);
    }

}
