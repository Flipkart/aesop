package com.flipkart.aesop.elasticsearchdatalayer.upsert;

import org.springframework.beans.factory.FactoryBean;
import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchConfig;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

/**
 * Generates objects of {@link ElasticSearchUpsertDataLayer } and ensures that it is singleton.
 * @author Jagadeesh Huliyar
 */
public class ElasticSearchUpsertDataLayerFactory implements FactoryBean<ElasticSearchUpsertDataLayer>
{
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchUpsertDataLayerFactory.class);
    public ElasticSearchConfig elasticSearchConfig;
    public ElasticSearchUpsertDataLayer es;
    public ConcurrentHashMap<String, Config> cachedConfigMap;

	public ElasticSearchUpsertDataLayer getObject() throws Exception
    {
        es =  new ElasticSearchUpsertDataLayer();
        cachedConfigMap = new ConcurrentHashMap<String, Config>();
        LOGGER.info("elasticSearchConfig: "+elasticSearchConfig.getConfig());
	    cachedConfigMap.putIfAbsent(elasticSearchConfig.getConfig(),
                ConfigFactory.parseFile(new File(elasticSearchConfig.getConfig()))) ;

        //LOGGER.info("elasticSearchConfig: "+ es.cachedConfigMap.get("config.infra.es.conf").getString("cluster.name"));
        es.initialise(cachedConfigMap.get("config.infra.es.conf"));
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

    public void setElasticSearchConfig(ElasticSearchConfig elasticSearchConfig)
    {
        this.elasticSearchConfig=elasticSearchConfig;
    }
}
