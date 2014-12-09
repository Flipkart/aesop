package com.flipkart.aesop.elasticsearchdatalayer.delete;


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
 * Generates objects of {@link ElasticSearchDeleteDataLayer} and ensures that it is singleton.
 * @author Jagadeesh Huliyar
 */
public class ElasticSearchDeleteDataLayerFactory implements FactoryBean<ElasticSearchDeleteDataLayer>
{
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchDeleteDataLayerFactory.class);

    public ElasticSearchDeleteDataLayer es;

    private ElasticSearchInitializer elasticSearchInitializer;

	public ElasticSearchDeleteDataLayer getObject() throws Exception
    {
        es =  new ElasticSearchDeleteDataLayer();
        es.elasticSearchInitializer = elasticSearchInitializer;

        LOGGER.info("elasticSearchConfig: "+es);
        return es;
    }

	public Class<?> getObjectType()
    {
	    return ElasticSearchDeleteDataLayer.class;
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
