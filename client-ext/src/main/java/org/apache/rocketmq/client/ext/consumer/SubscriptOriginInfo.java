package org.apache.rocketmq.client.ext.consumer;

import org.apache.rocketmq.common.filter.ExpressionType;

import java.util.HashSet;
import java.util.Set;

/**
 * @author saleson
 * @date 2022-05-18 17:23
 */
public class SubscriptOriginInfo {
    private String topic;
    private String subString;
    private String expressionType = ExpressionType.TAG;
}
