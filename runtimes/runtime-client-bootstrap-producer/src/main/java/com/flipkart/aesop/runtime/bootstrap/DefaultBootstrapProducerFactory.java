/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.runtime.bootstrap;

import java.util.Properties;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.aesop.runtime.config.BootstrapProducerConfig;
import com.flipkart.aesop.runtime.config.ClientConfig;
import com.linkedin.databus.bootstrap.producer.BootstrapProducerStaticConfig;
import com.linkedin.databus.core.util.ConfigLoader;

/**
 * The Spring factory bean for creating {@link DefaultBootstrapProducer} instances based on configured properties
 * 
 * @author Regunath B
 * @version 1.0, 06 Feb 2014
 */
public class DefaultBootstrapProducerFactory implements FactoryBean<DefaultBootstrapProducer>, InitializingBean {
	
	/** The configuration details for creating the Relay Client Bootstrap producer */
	private BootstrapProducerConfig bootstrapProducerConfig;
	
	/** The configuration details for creating the Relay Client*/
	private ClientConfig clientConfig;
	
    /**
     * Interface method implementation. Creates and returns a {@link DefaultBootstrapProducer} instance
     * @see org.springframework.beans.factory.FactoryBean#getObject()
     */
	public DefaultBootstrapProducer getObject() throws Exception {
		// using fully qualified class name for Databus BootstrapProducerConfig reference as we have class with same name in Aesop as well
		com.linkedin.databus.bootstrap.producer.BootstrapProducerConfig producerConfig = 
				new com.linkedin.databus.bootstrap.producer.BootstrapProducerConfig(); 
		ConfigLoader<BootstrapProducerStaticConfig> staticProducerConfigLoader = new ConfigLoader<BootstrapProducerStaticConfig>(
				BootstrapProducerConfig.BOOTSTRAP_PROPERTIES_PREFIX, producerConfig);
		// create a merged properties list from Relay Client and Bootstrap specific properties
		Properties mergedProperties = this.bootstrapProducerConfig.getRelayClientBootstrapProperties();
		mergedProperties.putAll(this.clientConfig.getClientProperties());		
		BootstrapProducerStaticConfig staticProducerConfig = staticProducerConfigLoader.loadConfig(mergedProperties);
	    return new DefaultBootstrapProducer(staticProducerConfig);	
	}

	/**
	 * Interface method implementation. Checks for mandatory dependencies 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.bootstrapProducerConfig,"'bootstrapProducerConfig' cannot be null. This Relay Client Bootstrap producer will not be initialized");
		Assert.notNull(this.clientConfig,"'clientConfig' cannot be null. This Relay Client Bootstrap producer will not be initialized");
	}
	
	/**
	 * Interface method implementation. Returns the DefaultBootstrapProducer type
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<DefaultBootstrapProducer> getObjectType() {
		return DefaultBootstrapProducer.class;
	}

	/**
	 * Interface method implementation. Returns true
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */	
	public boolean isSingleton() {
		return true;
	}

	/** Getter/Setter methods to override default implementations of various components used by this Relay Client*/
	public BootstrapProducerConfig getBootstrapProducerConfig() {
		return bootstrapProducerConfig;
	}
	public void setBootstrapProducerConfig(BootstrapProducerConfig bootstrapProducerConfig) {
		this.bootstrapProducerConfig = bootstrapProducerConfig;
	}
	public ClientConfig getClientConfig() {
		return this.clientConfig;
	}
	public void setClientConfig(ClientConfig clientConfig) {
		this.clientConfig = clientConfig;
	}
	
}

