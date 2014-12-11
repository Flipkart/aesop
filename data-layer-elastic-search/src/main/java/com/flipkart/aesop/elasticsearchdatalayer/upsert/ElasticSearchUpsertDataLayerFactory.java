package com.flipkart.aesop.elasticsearchdatalayer.upsert;

import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchDataLayerClient;
import org.springframework.beans.factory.FactoryBean;


/**
 * Generates objects of {@link ElasticSearchUpsertDataLayer } and ensures that it is singleton.
 * @author Pratyay Banerjee
 */
public class ElasticSearchUpsertDataLayerFactory implements FactoryBean<ElasticSearchUpsertDataLayer>
{
    /* Data Layer Client */
    private ElasticSearchDataLayerClient elasticSearchDataLayerClient;

    public ElasticSearchUpsertDataLayer getObject() throws Exception
    {
        ElasticSearchUpsertDataLayer elasticSearchUpsertDataLayer =  new ElasticSearchUpsertDataLayer();

        /* set the elasticSearchDataLayerClient, initiates the elasticSearch server */
        elasticSearchUpsertDataLayer.setElasticSearchDataLayerClient(elasticSearchDataLayerClient);

        return elasticSearchUpsertDataLayer;
    }

    public Class<?> getObjectType() {
        return ElasticSearchUpsertDataLayer.class;
    }

    public boolean isSingleton() {
        return true;
    }

    /* Getters and Setters Start */
    public ElasticSearchDataLayerClient getElasticSearchDataLayerClient() {
        return elasticSearchDataLayerClient;
    }

    public void setElasticSearchDataLayerClient(ElasticSearchDataLayerClient elasticSearchDataLayerClient) {
        this.elasticSearchDataLayerClient = elasticSearchDataLayerClient;
    }
    /* Getters and Setters End */
}
