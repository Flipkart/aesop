package com.flipkart.aesop.elasticsearchdatalayer.delete;

import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchDataLayerClient;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreOperation;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.core.DbusOpcode;


/**
 * ElasticSearch Delete Data Layer. Persists {@link DbusOpcode#DELETE} events to Log File.
 * @author Pratyay Banerjee
 * @see com.flipkart.aesop.elasticsearchdatalayer.upsert.ElasticSearchUpsertDataLayer
 */
public class ElasticSearchDeleteDataLayer extends DeleteDestinationStoreOperation
{
    /** Logger for this class*/
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchDeleteDataLayer.class);

    /* ES Data Layer Client. */
    private ElasticSearchDataLayerClient elasticSearchDataLayerClient;

    @Override
    protected void delete(AbstractEvent event)
    {
        LOGGER.info("Received Delete Event. Event is " + event);
        String id = String.valueOf(event.getFieldMapPair().get("id"));
        /* Prepare Delete Request and execute */
        elasticSearchDataLayerClient.getClient().prepareDelete(elasticSearchDataLayerClient.getIndex(),
                elasticSearchDataLayerClient.getType(), id)
                .execute()
                .actionGet();

        /* Check if source still exists*/
        try{
            GetResponse response = elasticSearchDataLayerClient.getClient().prepareGet(elasticSearchDataLayerClient.getIndex(),
                    elasticSearchDataLayerClient.getType(), id).execute().get();
            if(!response.isSourceEmpty()) {
                LOGGER.info("Delete Error:" + response);
            }
        }
        catch(Exception e)
        {
            LOGGER.info("Delete Error:" + e);
        }

    }

    /* Getters and Setters start */
    public void setElasticSearchDataLayerClient(ElasticSearchDataLayerClient elasticSearchDataLayerClient) {
        this.elasticSearchDataLayerClient = elasticSearchDataLayerClient;
    }
    /* Getters and Setters end */
}
