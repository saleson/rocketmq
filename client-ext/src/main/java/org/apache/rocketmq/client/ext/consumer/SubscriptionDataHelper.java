package org.apache.rocketmq.client.ext.consumer;

import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.ext.gray.GrayManager;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.util.Objects;

/**
 * @author saleson
 * @date 2022-05-18 22:05
 */
public class SubscriptionDataHelper {

    public static SubscriptionData transformToIncludeGray(String routingTag, SubscriptionData subscriptionData) {
        if (subscriptionData.isClassFilterMode()) {
            return subscriptionData;
        }
        StringBuilder newExpression = new StringBuilder();
        if (Objects.equals(ExpressionType.TAG, subscriptionData.getExpressionType())) {
            String subString = subscriptionData.getSubString();
            if (null == subString || subString.equals(SubscriptionData.SUB_ALL) || subString.length() == 0) {
                newExpression.append(buildRoutingTagSql(routingTag));
            } else {
                String inTagStr = subString.replaceAll("\\|\\|", "','");
                newExpression.append("(TAGS is not null and TAGS in ('")
                        .append(inTagStr)
                        .append("')")
                        .append(" and ")
                        .append(buildRoutingTagSql(routingTag));
            }
        } else {
            newExpression.append("(")
                    .append(subscriptionData.getSubString())
                    .append(")")
                    .append(" and ")
                    .append(buildRoutingTagSql(routingTag));
        }

        SubscriptionData newSubscriptionData = new SubscriptionData();
        newSubscriptionData.setTopic(subscriptionData.getTopic());
        newSubscriptionData.setSubString(newExpression.toString());
        newSubscriptionData.setExpressionType(ExpressionType.SQL92);
        return newSubscriptionData;
    }

    public static SubscriptionData transformToIncludeGray(String routingTag, String topic, MessageSelector messageSelector) {
        StringBuilder newExpression = new StringBuilder();
        if (Objects.equals(ExpressionType.TAG, messageSelector.getExpressionType())) {
            String expression = messageSelector.getExpression();
            if (null == expression || expression.equals(SubscriptionData.SUB_ALL) || expression.length() == 0) {
                newExpression.append(buildRoutingTagSql(routingTag));
            } else {
                String inTagStr = expression.replaceAll("\\|\\|", "','");
                newExpression.append("(TAGS is not null and TAGS in ('")
                        .append(inTagStr)
                        .append("')")
                        .append(" and ")
                        .append(buildRoutingTagSql(routingTag));
            }
        } else {
            newExpression.append("(")
                    .append(messageSelector.getExpression())
                    .append(")")
                    .append(" and ")
                    .append(buildRoutingTagSql(routingTag));
        }

        SubscriptionData newSubscriptionData = new SubscriptionData();
        newSubscriptionData.setTopic(topic);
        newSubscriptionData.setSubString(newExpression.toString());
        newSubscriptionData.setExpressionType(ExpressionType.SQL92);
        return newSubscriptionData;
    }

    private static String buildRoutingTagSql(String routingTag) {
        return GrayManager.MSG_ROUTING_TAG_KEY + "='" + routingTag + "'";
    }

}
