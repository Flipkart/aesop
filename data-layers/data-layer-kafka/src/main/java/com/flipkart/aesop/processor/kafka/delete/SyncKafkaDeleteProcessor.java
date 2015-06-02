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

import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreProcessor;
import com.flipkart.aesop.processor.kafka.preprocessor.KafkaEventDefaultPreprocessor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.processor.kafka.client.KafkaClient;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;

/**
 * Kafka Delete Event Data Layer. Persists {@link DbusOpcode#DELETE} events to Log File.
 *
 * @author Ravindra Yadav
 */
public class SyncKafkaDeleteProcessor extends DeleteDestinationStoreProcessor {
    /**
     * Logger for this class
     */
    private static final Logger LOGGER = LogFactory.getLogger(SyncKafkaDeleteProcessor.class);
    private KafkaEventDefaultPreprocessor kafkaEventDefaultPreprocessor;

    @Override
    protected ConsumerCallbackResult delete(AbstractEvent event) {
        LOGGER.info("Received Delete Event. Event is " + event);
        LOGGER.info("Field Map Pair : " + event.getFieldMapPair().toString());

        try {
            ProducerRecord record = kafkaEventDefaultPreprocessor.createProducerRecord(event);
            KafkaClient kafkaClient = kafkaEventDefaultPreprocessor.getKafkaClient();

            RecordMetadata response = (RecordMetadata) kafkaClient.getClient().send(record).get();

            if (response == null) {
                LOGGER.error("Kafka Send Error : " + response);
                return ConsumerCallbackResult.ERROR;
            } else {
                LOGGER.info("Send successful :  topic :: partition - " + response.topic() + "::" + response.partition());
                return ConsumerCallbackResult.SUCCESS;
            }
            }catch(Exception e){
                LOGGER.error("Kafka Server Connection Lost/Send Error" + e);
                return ConsumerCallbackResult.ERROR;
            }
        }

    public KafkaEventDefaultPreprocessor getKafkaEventDefaultPreprocessor() {
        return kafkaEventDefaultPreprocessor;
    }

    public void setKafkaEventDefaultPreprocessor(KafkaEventDefaultPreprocessor kafkaEventDefaultPreprocessor) {
        this.kafkaEventDefaultPreprocessor = kafkaEventDefaultPreprocessor;
    }

    }