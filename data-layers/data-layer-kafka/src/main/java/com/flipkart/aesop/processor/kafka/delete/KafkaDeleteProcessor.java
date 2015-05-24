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
package com.flipkart.aesop.processor.kafka.delete;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.processor.kafka.client.KafkaClient;
import com.linkedin.databus.core.DbusOpcode;

/**
 * Kafka Delete Data Layer. Persists {@link DbusOpcode#DELETE} events to Log File.
 * @author Ravindra Yadav
 */
public class KafkaDeleteProcessor extends DeleteDestinationStoreProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(KafkaDeleteProcessor.class);

	/* Kafka Data Layer Client. */
	private KafkaClient kafkaClient;

	@Override
	protected void delete(AbstractEvent event)
	{
		//KAFKA logs - delete not supported with the current version
	}

	/* Getters and Setters start */
	public void setKafkaClient(KafkaClient kafkaClient)
	{
		this.kafkaClient = kafkaClient;
	}

	public KafkaClient getKafkaClient()
	{
		return this.kafkaClient;
	}
	/* Getters and Setters end */
}
