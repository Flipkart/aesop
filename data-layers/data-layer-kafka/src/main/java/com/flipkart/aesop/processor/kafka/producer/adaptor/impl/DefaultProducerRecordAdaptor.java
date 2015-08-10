package com.flipkart.aesop.processor.kafka.producer.adaptor.impl;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.processor.kafka.KafkaDataLayerConstants;
import com.flipkart.aesop.processor.kafka.producer.adaptor.ProducerRecordAdaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultProducerRecordAdaptor extends ProducerRecordAdaptor<String,Map<String,Object>> {

    @Override
    public String getTopicName(AbstractEvent event) {
        return event.getNamespaceName()+KafkaDataLayerConstants.DEFAULT_KAFKA_SUFFIX;
    }

    @Override
    public String getPrimaryKey(AbstractEvent event) {

        if (event.getPrimaryKeyValues() != null) {
            if(event.getPrimaryKeyValues().size() == 1 )
                return String.valueOf(event.getPrimaryKeyValues().get(0));
            else {
                StringBuilder keyBuilder = new StringBuilder();
                List<Object> primaryKeys = event.getPrimaryKeyValues();
                for(Object object : primaryKeys) {
                    keyBuilder.append(String.valueOf(object)).append("_");
                }
                /* Removing the last extra _ and returnig the key */
                return keyBuilder.substring(0,keyBuilder.length()-1);
            }
        } else {
            /*
            If key not found in event this method returns null , relies on the client to partition appropriately
             */
            return null;
        }
    }

    @Override
    public Map<String, Object> getPayload(AbstractEvent event) {
        Object key = getPrimaryKey(event);
        Map<String, Object> fieldMap = event.getFieldMapPair();
        String entityName = event.getEntityName();
        String nameSpaceName = event.getNamespaceName();
        String opCode = event.getEventType().name();

        Map<String, Object> payload = new HashMap<String, Object>();
        payload.put(KafkaDataLayerConstants.KEY, key);
        payload.put(KafkaDataLayerConstants.FIELD_MAP, fieldMap);
        payload.put(KafkaDataLayerConstants.ENTITY, entityName);
        payload.put(KafkaDataLayerConstants.NAMESPACE, nameSpaceName);
        payload.put(KafkaDataLayerConstants.OPCODE, opCode);
        return payload;
    }
}
