package com.flipkart.aesop.elasticsearchdatalayer.upsert;


import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchInitializer;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreOperation;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.core.DbusOpcode;
import org.elasticsearch.action.index.IndexResponse;

/**
 * ElasticSearch Upsert Data Layer. Persists {@link DbusOpcode#UPSERT} events to Logs.
 * @author Pratyay Banerjee
 * @see com.flipkart.aesop.elasticsearchdatalayer.delete.ElasticSearchDeleteDataLayer
 */
public class ElasticSearchUpsertDataLayer extends UpsertDestinationStoreOperation
{
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchUpsertDataLayer.class);

    /* ES Initializer Client. */

    public ElasticSearchInitializer elasticSearchInitializer;

	@Override
	protected void upsert(AbstractEvent event)
	{

		LOGGER.info("Received Upsert Event. Event is " + event);
        LOGGER.info("Field Map Pair : " + event.getFieldMapPair().toString());

        try {
            String id = String.valueOf(event.getFieldMapPair().get("id"));
            //delete if "id" exists
            elasticSearchInitializer.client.prepareDelete(elasticSearchInitializer.getNamespace(),elasticSearchInitializer.getIndex(),id)
                    .execute()
                    .actionGet();
            //create the new id
            IndexResponse response = elasticSearchInitializer.client.prepareIndex(elasticSearchInitializer.getNamespace(),elasticSearchInitializer.getIndex(),id)
                    .setSource(event.getFieldMapPair())
                    .execute()
                    .get();
            if(!response.isCreated())  {
                LOGGER.info("Create Error : " + response);
            }
        } catch (Exception e) {
            LOGGER.info("Create Error : " + e);
        }

	}
}
