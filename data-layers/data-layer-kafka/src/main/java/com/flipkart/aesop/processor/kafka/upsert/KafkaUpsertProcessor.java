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
package com.flipkart.aesop.processor.kafka.upsert;

import java.util.List;
import java.util.concurrent.Future;

import com.flipkart.aesop.processor.kafka.client.KafkaClient;
import com.flipkart.aesop.processor.kafka.config.KafkaConfig;
import com.flipkart.aesop.processor.kafka.preprocessor.KafkaUpsertPreprocessor;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.util.SerializationUtils;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;

import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;


/**
 * Kafka Upsert Data Layer. Persists {@link DbusOpcode#UPSERT} events to Logs.
 * @author Ravindra Yadav
 * @see com.flipkart.aesop.processor.kafka.delete.KafkaDeleteProcessor
 */
public class KafkaUpsertProcessor extends KafkaUpsertPreprocessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(KafkaUpsertProcessor.class);

	@Override
	protected ConsumerCallbackResult upsert(AbstractEvent event)
	{
		LOGGER.info("Received Upsert Event. Event is " + event);
		LOGGER.info("Field Map Pair : " + event.getFieldMapPair().toString());

		try
		{
			ProducerRecord record = createProducerRecord(event);
			KafkaClient kafkaClient = getKafkaClient();

			//To make a blocking send
			boolean isSync = kafkaClient.isSync(event.getNamespaceName());

			if( isSync )
			{
				RecordMetadata response = (RecordMetadata)kafkaClient.getClient().send(record).get();

				if (response == null )
				{
					LOGGER.error("Send Error : " + response);
//					throw new RuntimeException("Send Failure");
					return ConsumerCallbackResult.ERROR;
				}
				else
				{
					LOGGER.info("Send successful :  topic :: partition - " + response.topic() + "::" + response.partition());
					return ConsumerCallbackResult.SUCCESS;
				}
			}
			else
			{
				Future<RecordMetadata> response = kafkaClient.getClient().send(record);
				if (response == null  || !response.isDone())
				{
					LOGGER.error("Send Error : " + response);
//					throw new RuntimeException("Send Failure");
					return ConsumerCallbackResult.ERROR;
				}
				else
				{
					LOGGER.info("Send successful :  topic :: partition - " + response.get().topic() + "::" + response.get().partition());
					return ConsumerCallbackResult.SUCCESS;
				}
			}

		}
		catch (Exception e)
		{
			LOGGER.info("Server Connection Lost/Send Error" + e);
			throw new RuntimeException("Send Failure");
		}
	}

}
