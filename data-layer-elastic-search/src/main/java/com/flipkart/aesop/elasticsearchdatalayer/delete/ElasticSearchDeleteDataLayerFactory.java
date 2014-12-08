package com.flipkart.aesop.elasticsearchdatalayer.delete;


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
    public ElasticSearchConfig elasticSearchConfig;
    public ElasticSearchDeleteDataLayer es;
    public ConcurrentHashMap<String, Config> cachedConfigMap;

	public ElasticSearchDeleteDataLayer getObject() throws Exception
    {
        es =  new ElasticSearchDeleteDataLayer();
        cachedConfigMap = new ConcurrentHashMap<String, Config>();
        LOGGER.info("elasticSearchConfig: "+elasticSearchConfig.getConfig());
        cachedConfigMap.putIfAbsent(elasticSearchConfig.getConfig(),
                ConfigFactory.parseFile(new File(elasticSearchConfig.getConfig()))) ;

        //LOGGER.info("elasticSearchConfig: "+ es.cachedConfigMap.get("config.infra.es.conf").getString("cluster.name"));
       // es.initialise(cachedConfigMap.get("config.infra.es.conf"));
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

    public void setElasticSearchConfig(ElasticSearchConfig elasticSearchConfig)
    {
        this.elasticSearchConfig=elasticSearchConfig;
    }
}
