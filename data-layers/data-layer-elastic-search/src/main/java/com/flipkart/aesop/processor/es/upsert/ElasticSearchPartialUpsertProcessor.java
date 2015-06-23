package com.flipkart.aesop.processor.es.upsert;

import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.processor.es.client.ElasticSearchClient;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * <code>ElasticSearchPartialUpsertProcessor</code> is an elastic search upsert data layer which allows partial updates to an
 * existing document (based on simple recursive merge, inner merging of objects,
 * replacing core "keys/values" and arrays) / indexing a new document in-case
 * it doesn't exist.<br/><br/>
 *
 * @author Durga Prasana
 */

public class ElasticSearchPartialUpsertProcessor extends UpsertDestinationStoreProcessor
{
    /** Logger for this class*/
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchPartialUpsertProcessor.class);

    /* ES Initializer Client. */
    private ElasticSearchClient elasticSearchClient;

    @Override
    protected ConsumerCallbackResult upsert(AbstractEvent event)
    {

        LOGGER.info("Received Partial Upsert Event : " + event);
        LOGGER.info("Field Map Pair : " + event.getFieldMapPair().toString());

        try {

            String docId = String.valueOf(event.getFieldMapPair().get("id"));
            String index = isBlank(event.getNamespaceName()) ? elasticSearchClient.getIndex() : event.getNamespaceName();
            String type = isBlank(event.getEntityName()) ? elasticSearchClient.getType() : event.getEntityName();

            IndexRequest indexReq = new IndexRequest(index, type, docId).
                    source(event.getFieldMapPair());

            UpdateRequest updateReq = new UpdateRequest(index, type, docId).
                    doc(event.getFieldMapPair()).
                    upsert(indexReq);

            UpdateResponse response = elasticSearchClient.getClient().update(updateReq).get();

            LOGGER.info("Created/Updated doc: " + response.getId() + " v: " + response.getVersion() + " isCreated: " + response.isCreated());

        } catch (Exception e) {

            LOGGER.info("Server Connection Lost/Create Error" + e);
            throw new RuntimeException("Create Failure");
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
