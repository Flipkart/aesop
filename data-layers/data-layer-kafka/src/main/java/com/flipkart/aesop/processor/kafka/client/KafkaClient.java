/*******************************************************************************
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.flipkart.aesop.processor.kafka.client;

import com.flipkart.aesop.processor.kafka.config.KafkaConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.InitializingBean;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.io.File;
import java.util.Properties;

/**
 * Initiates Kafka Client , reads config from KafkaConfig
 *
 * @author Ravindra Yadav
 */
public class KafkaClient implements InitializingBean {

    /* Kafka Config set by spring-beans */
    protected KafkaConfig kafkaConfig;

    /* Aesop Config Instance */
    protected Config config;

    /* This variable denotes the Kafka server client */
    protected KafkaProducer client;

    /**
     * This method is called from {@link org.springframework.beans.factory.InitializingBean#afterPropertiesSet()} to
     * initialize the Kafka Client
     */
    private static final Logger LOGGER = LogFactory.getLogger(KafkaClient.class);

    void init() {
        this.config = ConfigFactory.parseFile(new File(kafkaConfig.getConfig()));

        Properties props = new Properties();
        props.put("zookeeper.connect", config.getString("zookeeper.connect"));
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("zk.connectiontimeout.ms", config.getString("zk.connectiontimeout.ms"));
        props.put("bootstrap.servers", config.getString("bootstrap.servers"));

        this.client = new KafkaProducer(props);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    /* Getters and Setters Start */
    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public KafkaProducer getClient() {
        return client;
    }
    /* Getters and Setters End */

}
