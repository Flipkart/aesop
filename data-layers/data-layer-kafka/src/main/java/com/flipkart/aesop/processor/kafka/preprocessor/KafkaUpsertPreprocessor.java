package com.flipkart.aesop.processor.kafka.preprocessor;

import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.processor.kafka.client.KafkaClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.util.SerializationUtils;

import java.util.List;

/**
 * Created by r.yadav on 10/04/15.
 */
public abstract class KafkaUpsertPreprocessor extends UpsertDestinationStoreProcessor {

    /* Kafka Initializer Client. */
    private KafkaClient kafkaClient;

    public ProducerRecord createProducerRecord(AbstractEvent event)
    {
        /*Kafka Record is created assuming default key based partitioning
         * String serialiation has been used for key and value
         * Topic is configured via properties file
         */
        ProducerRecord record = new ProducerRecord(kafkaClient.getTopic(event.getNamespaceName()), getPrimaryKey(event),SerializationUtils.serialize(event
                .getFieldMapPair()));

        return record;
    }

    /*
     * Helper method to extract primary key from event
     */
    private Object getPrimaryKey(AbstractEvent event)
    {
        /*Picks the first element from the list
         * This could be modified further based on requirements
         */
        if( event.getPrimaryKeyValues() != null && event.getPrimaryKeyValues().size() > 0 )
        {
            return event.getPrimaryKeyValues().get(0);
        }
        else
        {
            /*
            If key not found in event this method returns null , relies on the client to partition appropriately
             */
            return null;
        }
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
