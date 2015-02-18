/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *******************************************************************************/
package com.flipkart.aesop.processor.es.client;

import com.flipkart.aesop.processor.es.config.ElasticSearchConfig;
import com.typesafe.config.Config;
import org.elasticsearch.client.Client;
import org.springframework.beans.factory.InitializingBean;

/**
 * Initiates ElasticSearch Client , reads config from ElasticSearchConfig
 * @author Pratyay Banerjee
 */
public abstract class ElasticSearchClient implements InitializingBean {

    /* Elastic Search Config set by spring-beans */
    protected ElasticSearchConfig elasticSearchConfig;

    /* Aesop Config Instance */
    protected Config config;

    /* This variable denotes the Elastic Search server client */
    protected Client client;

    /**
     * This method is called from {@link org.springframework.beans.factory.InitializingBean#afterPropertiesSet()} to
     * initialize the Elastic Search Client
     */
    abstract void init();

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    /* Getters and Setters Start */
    public ElasticSearchConfig getElasticSearchConfig() {
        return elasticSearchConfig;
    }

    public void setElasticSearchConfig(ElasticSearchConfig elasticSearchConfig) {
        this.elasticSearchConfig = elasticSearchConfig;
    }

    public String getIndex() {
        return  config.getString("cluster.index");
    }

    public String getType() {
        return config.getString("cluster.type");
    }

    public Client getClient() {
        return client;
    }
    /* Getters and Setters End */


}
