package com.flipkart.aesop.processor.kafka.processor;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.processor.DestinationEventProcessor;
import com.flipkart.aesop.processor.kafka.client.KafkaClient;
import com.flipkart.aesop.processor.kafka.producer.adaptor.ProducerRecordAdaptor;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;


/**
 * Created by arya.ketan on 10/08/15.
 */
public abstract class KafkaEventProcessor implements DestinationEventProcessor {
    /* Kafka Initializer Client. */
    protected KafkaClient kafkaClient;

    /**
     * Logger for this class
     */
    protected static final Logger LOGGER = LogFactory.getLogger(KafkaEventProcessor.class);

    /*
     * Used for creating kafka payload.
     */
    private ProducerRecordAdaptor producerRecordAdaptor;

    @SuppressWarnings(value = "unchecked")
    @Override
    public ConsumerCallbackResult processDestinationEvent(AbstractEvent event) {
        LOGGER.info("Received Abstract Event. Event is " + event);

        try {
            return submitRecord(producerRecordAdaptor.createProducerRecord(event));
        } catch (Exception e) {
            LOGGER.error("Kafka Server Connection Lost/Send Error" + e);
            return ConsumerCallbackResult.ERROR;
        }
    }

    /**
     * This method is to be overridden by Async or the Sync Producers.
     * @param producerRecord the Producer Record
     * @return the CallbackResult
     * @throws Exception
     */
    public abstract ConsumerCallbackResult submitRecord(ProducerRecord producerRecord) throws Exception;

    /* Getters & Setters start */
    public ProducerRecordAdaptor getProducerRecordAdaptor() {
        return producerRecordAdaptor;
    }

    public void setProducerRecordAdaptor(ProducerRecordAdaptor producerRecordAdaptor) {
        this.producerRecordAdaptor = producerRecordAdaptor;
    }

    public void setKafkaClient(KafkaClient kafkaClient) {
        this.kafkaClient = kafkaClient;
    }

    public KafkaClient getKafkaClient() {
        return this.kafkaClient;
    }

     /* Getters & Setters end */
}
