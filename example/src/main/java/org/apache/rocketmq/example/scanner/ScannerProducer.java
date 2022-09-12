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
package org.apache.rocketmq.example.scanner;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class ScannerProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        System.out.printf("Producer Started.%n");

        Scanner scanner = new Scanner(System.in);
        while (true) {
            String txt = scanner.next();
            if (txt.equalsIgnoreCase("exit")) {
                break;
            }
            ScanMsgItem msgItem = new ScanMsgItem(txt);
            List<String> keys = Arrays.asList(msgItem.getKeys().split(","));
            System.out.printf("msgItem.keys: %s, keyList:%s %n", msgItem.getKeys(), keys);
            for (int i = 0; i < 100; i++) {
                try {
                    Message msg = new Message("AAATopicTestA",
                            msgItem.getTag(),
                            msgItem.getKeys().replaceAll(",", " "),
                            (msgItem.getBody() + i).getBytes(RemotingHelper.DEFAULT_CHARSET));


//                    msg.setKeys(keys);
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

//            try {
//                Message msg = new Message("TopicTest",
//                        msgItem.getTag(),
//                        msgItem.getKeys(),
//                        msgItem.getBody().getBytes(RemotingHelper.DEFAULT_CHARSET));
//
//                SendResult sendResult = producer.send(msg);
//                System.out.printf("%s%n", sendResult);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }

        }

        producer.shutdown();
    }

    private static class ScanMsgItem {

        public ScanMsgItem(String str) {
            String[] itmes = str.split(";");
            if (itmes.length == 1) {
                body = itmes[0];
                keys = "";
                tag = "";
            } else if (itmes.length == 2) {
                body = itmes[1];
                keys = itmes[0];
                tag = "";
            } else if (itmes.length == 3) {
                body = itmes[2];
                keys = itmes[0];
                tag = itmes[1];
            }
        }


        private String body;
        private String keys;
        private String tag;

        public String getBody() {
            return body;
        }

        public String getKeys() {
            return keys;
        }

        public String getTag() {
            return tag;
        }
    }
}
