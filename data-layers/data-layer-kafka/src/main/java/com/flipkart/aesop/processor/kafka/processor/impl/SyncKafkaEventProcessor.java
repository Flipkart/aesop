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
package com.flipkart.aesop.processor.kafka.processor.impl;

import com.flipkart.aesop.processor.kafka.processor.KafkaEventProcessor;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * Kafka Event Processor Data Layer. Persists events to Kafka in Sync.
 *
 * @author Ravindra Yadav
 * @see com.flipkart.aesop.processor.kafka.processor.KafkaEventProcessor
 */
public class SyncKafkaEventProcessor extends KafkaEventProcessor {

    @Override
    @SuppressWarnings(value = "unchecked")
    public ConsumerCallbackResult submitRecord(ProducerRecord producerRecord) throws Exception{

        RecordMetadata  response = (RecordMetadata) kafkaClient.getClient().send(producerRecord).get();
        if (response == null) {
            LOGGER.error("Kafka Send Error : NULL");
            return ConsumerCallbackResult.ERROR;
        } else {
            LOGGER.info("Send successful :  topic :: partition - " + response.topic() + "::" + response.partition());
            return ConsumerCallbackResult.SUCCESS;
        }
    }
}
