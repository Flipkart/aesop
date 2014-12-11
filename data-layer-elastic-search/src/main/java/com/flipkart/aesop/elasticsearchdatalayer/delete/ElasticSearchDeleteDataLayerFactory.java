package com.flipkart.aesop.elasticsearchdatalayer.delete;


import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchDataLayerClient;
import org.springframework.beans.factory.FactoryBean;


/**
 * Generates objects of {@link ElasticSearchDeleteDataLayer} and ensures that it is singleton.
 * @author Pratyay Banerjee
 */
public class ElasticSearchDeleteDataLayerFactory implements FactoryBean<ElasticSearchDeleteDataLayer>
{
    /* Data Layer Client */
    private ElasticSearchDataLayerClient elasticSearchDataLayerClient;

    public ElasticSearchDeleteDataLayer getObject() throws Exception
    {
        ElasticSearchDeleteDataLayer  elasticSearchDeleteDataLayer =  new ElasticSearchDeleteDataLayer();

        /* set the elasticSearchDataLayerClient */
        elasticSearchDeleteDataLayer.setElasticSearchDataLayerClient(elasticSearchDataLayerClient);

        return elasticSearchDeleteDataLayer;
    }

    public Class<?> getObjectType() {
        return ElasticSearchDeleteDataLayer.class;
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
