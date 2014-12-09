package com.flipkart.aesop.elasticsearchdatalayer.upsert;

import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchInitializer;
import org.springframework.beans.factory.FactoryBean;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;



/**
 * Generates objects of {@link ElasticSearchUpsertDataLayer } and ensures that it is singleton.
 * @author Pratyay Banerjee
 */
public class ElasticSearchUpsertDataLayerFactory implements FactoryBean<ElasticSearchUpsertDataLayer>
{
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchUpsertDataLayerFactory.class);

    public ElasticSearchUpsertDataLayer elasticSearchUpsertDataLayer;
    private ElasticSearchInitializer elasticSearchInitializer;

	public ElasticSearchUpsertDataLayer getObject() throws Exception
    {
        elasticSearchUpsertDataLayer =  new ElasticSearchUpsertDataLayer();
        //set the elasticSearchInitializer, initiates the elasticSearch server
        elasticSearchUpsertDataLayer.elasticSearchInitializer = elasticSearchInitializer;

        return elasticSearchUpsertDataLayer;
    }

	public Class<?> getObjectType()
    {
	    return ElasticSearchUpsertDataLayer.class;
    }

	public boolean isSingleton()
    {
	    return true;
    }

    public ElasticSearchInitializer getElasticSearchInitializer() {
        return elasticSearchInitializer;
    }

    public void setElasticSearchInitializer(ElasticSearchInitializer elasticSearchInitializer) {
        this.elasticSearchInitializer = elasticSearchInitializer;
    }
}
