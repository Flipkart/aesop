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
package com.flipkart.aesop.processor.kafka.processor;

import com.flipkart.aesop.processor.DestinationEventProcessor;
import com.flipkart.aesop.processor.kafka.client.KafkaClient;
import com.flipkart.aesop.processor.kafka.preprocessor.KafkaEventDefaultPreprocessor;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;

import com.flipkart.aesop.event.AbstractEvent;


/**
 * Kafka Event Data Layer. Persists events to Kafka asynchronously.
 *
 * @author Ravindra Yadav
 * @see com.flipkart.aesop.processor.kafka.processor.AsycKafkaEventProcessor
 */
public class AsycKafkaEventProcessor implements DestinationEventProcessor {
    /**
     * Logger for this class
     */
    private static final Logger LOGGER = LogFactory.getLogger(AsycKafkaEventProcessor.class);
    private KafkaEventDefaultPreprocessor kafkaEventDefaultPreprocessor;


    @Override
    public ConsumerCallbackResult processDestinationEvent(AbstractEvent event) {
        LOGGER.info("Received Abstract Event. Event is " + event);

        try {
            ProducerRecord record = kafkaEventDefaultPreprocessor.createProducerRecord(event);
            KafkaClient kafkaClient = kafkaEventDefaultPreprocessor.getKafkaClient();

            kafkaClient.getClient().send(record);

            //Callback not provided with this implementation
            LOGGER.info("Async Send successful ");
            return ConsumerCallbackResult.SUCCESS;


        } catch (Exception e) {
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
