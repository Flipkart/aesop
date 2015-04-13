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

import org.springframework.beans.factory.InitializingBean;

import com.flipkart.aesop.processor.kafka.config.KafkaConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.Properties;
import java.io.File;


/**
 * Initiates Kafka Client Impl 
 * @author Ravindra Yadav
 */
public class KafkaClientImpl extends KafkaClient
{
	private static final Logger LOGGER = LogFactory.getLogger(KafkaClientImpl.class);

	 @Override
	    void init() {
	        this.config = ConfigFactory.parseFile(new File(kafkaConfig.getConfig()));
	        
	        Properties props = new Properties();
			props.put("zk.connect", config.getString("zk.connect"));
			props.put("serializer.class", config.getString("serializer.class"));
			props.put("zk.connectiontimeout.ms", config.getString("zk.connectiontimeout.ms"));
			props.put("bootstrap.servers", config.getString("bootstrap.servers"));

	        KafkaProducer client = new KafkaProducer(props);
	        this.client = client;
	    }
}
