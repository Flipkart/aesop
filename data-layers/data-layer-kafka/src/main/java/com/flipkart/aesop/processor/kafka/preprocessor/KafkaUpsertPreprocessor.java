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
        String topic = kafkaClient.getTopic(event.getEntityName());
        int partitionForKey = getKafkaPartitionForKey(event.getPrimaryKeyValues());
        ProducerRecord record = new ProducerRecord(topic,partitionForKey, SerializationUtils.serialize(event.getPrimaryKeyValues().get(0)),SerializationUtils.serialize(event
                .getFieldMapPair()));

        return record;

    }

    public int getKafkaPartitionForKey(List<Object> primaryKeyValues) {
        return 1;
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
