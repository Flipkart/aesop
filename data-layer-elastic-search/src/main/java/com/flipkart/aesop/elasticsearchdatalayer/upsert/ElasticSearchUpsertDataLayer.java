/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *******************************************************************************/
package com.flipkart.aesop.elasticsearchdatalayer.upsert;

import com.flipkart.aesop.elasticsearchdatalayer.elasticsearchclient.ElasticSearchClient;
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
    private ElasticSearchClient elasticSearchClient;

    @Override
    protected void upsert(AbstractEvent event)
    {

        LOGGER.info("Received Upsert Event. Event is " + event);
        LOGGER.info("Field Map Pair : " + event.getFieldMapPair().toString());

        try {
            String id = String.valueOf(event.getFieldMapPair().get("id"));
            //delete if "id" exists
            elasticSearchClient.getClient().prepareDelete(elasticSearchClient.getIndex(),
                    elasticSearchClient.getType(), id)
                    .execute()
                    .actionGet();
            //create the new id
            IndexResponse response = elasticSearchClient.getClient().prepareIndex(elasticSearchClient.getIndex(),
                    elasticSearchClient.getType(), id)
                    .setSource(event.getFieldMapPair())
                    .execute()
                    .get();
            if(!response.isCreated())  {
                LOGGER.info("Create Error : " + response);
            }
        } catch (Exception e) {
            LOGGER.info("Create Error : " + e);
            throw new RuntimeException("Server Down Error: Unable To get Host Information");
        }
    }

    /* Getters and Setters start */
    public void setElasticSearchClient(ElasticSearchClient elasticSearchClient) {
        this.elasticSearchClient = elasticSearchClient;
    }
    /* Getters and Setters end */
}
