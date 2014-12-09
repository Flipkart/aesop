package com.flipkart.aesop.elasticsearchdatalayer.upsert;

import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchInitializer;
import org.springframework.beans.factory.FactoryBean;
import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchConfig;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Generates objects of {@link ElasticSearchUpsertDataLayer } and ensures that it is singleton.
 * @author Jagadeesh Huliyar
 */
public class ElasticSearchUpsertDataLayerFactory implements FactoryBean<ElasticSearchUpsertDataLayer>
{
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchUpsertDataLayerFactory.class);

    public ElasticSearchUpsertDataLayer es;
    private ElasticSearchInitializer elasticSearchInitializer;

	public ElasticSearchUpsertDataLayer getObject() throws Exception
    {
        es =  new ElasticSearchUpsertDataLayer();
        es.elasticSearchInitializer = elasticSearchInitializer;
        LOGGER.info("elasticSearchConfig: "+es);

        return es;
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
