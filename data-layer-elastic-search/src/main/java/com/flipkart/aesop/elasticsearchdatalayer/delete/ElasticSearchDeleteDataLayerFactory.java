package com.flipkart.aesop.elasticsearchdatalayer.delete;

import org.springframework.beans.factory.FactoryBean;
import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchConfig;

import org.springframework.beans.factory.InitializingBean;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

/**
 * Generates objects of {@link ElasticSearchDeleteDataLayer} and ensures that it is singleton.
 * @author Jagadeesh Huliyar
 */
public class ElasticSearchDeleteDataLayerFactory implements FactoryBean<ElasticSearchDeleteDataLayer>,InitializingBean
{
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchDeleteDataLayerFactory.class);
    public ElasticSearchConfig elasticSearchConfig;
	public ElasticSearchDeleteDataLayer getObject() throws Exception
    {
        LOGGER.info("elasticSearchConfig"+elasticSearchConfig.config);

	    return new ElasticSearchDeleteDataLayer();
    }

	public Class<?> getObjectType()
    {
	    return ElasticSearchDeleteDataLayer.class;
    }

	public boolean isSingleton()
    {
	    return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
    public void setElasticSearchConfig(ElasticSearchConfig elasticSearchConfig)
    {
        this.elasticSearchConfig=elasticSearchConfig;
    }
}
