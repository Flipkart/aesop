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
package com.flipkart.aesop.processor.es.delete;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreProcessor;
import com.flipkart.aesop.processor.es.client.ElasticSearchClient;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusOpcode;
import org.elasticsearch.action.get.GetResponse;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;


/**
 * ElasticSearch Delete Data Layer. Persists {@link DbusOpcode#DELETE} events to Log File.
 * @author Pratyay Banerjee
 * @see com.flipkart.aesop.processor.es.upsert.ElasticSearchUpsertProcessor
 */
public class ElasticSearchDeleteProcessor extends DeleteDestinationStoreProcessor
{
    /** Logger for this class*/
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchDeleteProcessor.class);

    /* ES Data Layer Client. */
    private ElasticSearchClient elasticSearchClient;

    @Override
    protected ConsumerCallbackResult delete(AbstractEvent event)
    {
        LOGGER.info("Received Delete Event. Event is " + event);
        LOGGER.info("Field Map Pair : " + event.getFieldMapPair().toString());

        String id = String.valueOf(event.getFieldMapPair().get("id"));

        /* Prepare Delete Request and execute */
        elasticSearchClient.getClient().prepareDelete(elasticSearchClient.getIndex(),
             elasticSearchClient.getType(), id)
             .execute()
             .actionGet();

        /* Check if source still exists*/
        try{
            GetResponse response = elasticSearchClient.getClient().prepareGet(elasticSearchClient.getIndex(),
                    elasticSearchClient.getType(), id).execute().get();
            if(!response.isSourceEmpty()) {
                LOGGER.info("Delete Error:" + response);
                throw new RuntimeException("Delete Failure");
            }
        }
        catch(Exception e)
        {
            LOGGER.info("Server Connection Lost/Delete Error" + e);
            throw new RuntimeException("Delete Failure");
        }
        return ConsumerCallbackResult.SUCCESS;

    }

    /* Getters and Setters start */
    public void setElasticSearchClient(ElasticSearchClient elasticSearchClient) {
        this.elasticSearchClient = elasticSearchClient;
    }

    public ElasticSearchClient getElasticSearchClient() {
        return this.elasticSearchClient;
    }
    /* Getters and Setters end */
}
