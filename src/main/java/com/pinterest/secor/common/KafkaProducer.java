/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.common;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.pinterest.secor.common.SecorConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * KafkaProducer is a class, which
 *
 * @author Kunal Nawale (nkunal@gmail.com)
 */
public class KafkaProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);

    private SecorConfig mConfig;


    public KafkaProducer(SecorConfig config) {
        mConfig = config;
    }

    public void sendMessage(String msg) {
        String[] hosts = mConfig.getKafkaSeedBrokerHost();
        int port = mConfig.getKafkaSeedBrokerPort();
        StringBuilder brokerList = new StringBuilder();
        for (String host : hosts) {
        	if (brokerList.length() > 0) {
        		brokerList.append(',');
        	}
        	brokerList.append(host + ":" + port);
        }
    	Properties properties = new Properties();
        properties.put("metadata.broker.list", brokerList.toString());
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("producer.type","sync");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String,String> producer = new Producer<String, String>(producerConfig);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("archived_hours", msg);
        producer.send(data);
        producer.close();
    }
}
