package com.flipkart.aesop.elasticsearchdatalayer.delete;


import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchInitializer;
import org.springframework.beans.factory.FactoryBean;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;



/**
 * Generates objects of {@link ElasticSearchDeleteDataLayer} and ensures that it is singleton.
 * @author Pratyay Banerjee
 */
public class ElasticSearchDeleteDataLayerFactory implements FactoryBean<ElasticSearchDeleteDataLayer>
{
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchDeleteDataLayerFactory.class);

    public ElasticSearchDeleteDataLayer elasticSearchDeleteDataLayer;

    private ElasticSearchInitializer elasticSearchInitializer;

	public ElasticSearchDeleteDataLayer getObject() throws Exception
    {
        elasticSearchDeleteDataLayer =  new ElasticSearchDeleteDataLayer();

        //set the elasticSearchInitializer
        elasticSearchDeleteDataLayer.elasticSearchInitializer = elasticSearchInitializer;

        return elasticSearchDeleteDataLayer;
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
