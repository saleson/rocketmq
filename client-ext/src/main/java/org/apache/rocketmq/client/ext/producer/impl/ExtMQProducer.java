package org.apache.rocketmq.client.ext.producer.impl;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.ext.producer.MessageTransformer;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author saleson
 * @date 2022-05-18 15:25
 */
public class ExtMQProducer extends DefaultMQProducer {

    private MessageTransformer messageTransformer;

    public ExtMQProducer(String producerGroup, MessageTransformer messageTransformer) {
        super(producerGroup);
        this.messageTransformer = messageTransformer;
    }

    @Override
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msg));
    }

    @Override
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msg), timeout);
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msg), mq);
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msg), mq, timeout);
    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msg), selector, arg);
    }

    @Override
    public SendResult send(Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msgs));
    }

    @Override
    public SendResult send(Collection<Message> msgs, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msgs), timeout);
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msgs), messageQueue);
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msgs), messageQueue, timeout);
    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return super.send(transform(msg), selector, arg, timeout);
    }

    @Override
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        super.send(transform(msg), sendCallback);
    }

    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        super.send(transform(msg), sendCallback, timeout);
    }

    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        super.send(transform(msg), mq, sendCallback);
    }

    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        super.send(transform(msg), mq, sendCallback, timeout);
    }

    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        super.send(transform(msg), selector, arg, sendCallback);
    }

    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        super.send(transform(msg), selector, arg, sendCallback, timeout);
    }

    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        super.send(transform(msgs), sendCallback);
    }

    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        super.send(transform(msgs), sendCallback, timeout);
    }

    @Override
    public void send(Collection<Message> msgs, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        super.send(transform(msgs), mq, sendCallback);
    }

    @Override
    public void send(Collection<Message> msgs, MessageQueue mq, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        super.send(transform(msgs), mq, sendCallback, timeout);
    }

    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, Object arg) throws MQClientException {
        return super.sendMessageInTransaction(transform(msg), arg);
    }

    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, Object arg) throws MQClientException {
        return super.sendMessageInTransaction(transform(msg), tranExecuter, arg);
    }

    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        super.sendOneway(transform(msg));
    }

    @Override
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        super.sendOneway(transform(msg), mq);
    }

    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, InterruptedException {
        super.sendOneway(transform(msg), selector, arg);
    }


    private Message transform(Message msg) {
        if (Objects.isNull(messageTransformer)) {
            return msg;
        }
        return messageTransformer.transform(msg);
    }

    private Collection<Message> transform(Collection<Message> msgs) {
        if (Objects.isNull(messageTransformer)) {
            return msgs;
        }
        return msgs.stream()
                .map(messageTransformer::transform)
                .collect(Collectors.toList());
    }

}
