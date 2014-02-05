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
package org.aesop.runtime.bootstrap;

import java.util.Properties;

import org.aesop.runtime.config.BootstrapConfig;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.linkedin.databus.bootstrap.producer.BootstrapProducerConfig;
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
	private BootstrapConfig bootstrapConfig;
	
    /**
     * Interface method implementation. Creates and returns a {@link DefaultBootstrapProducer} instance
     * @see org.springframework.beans.factory.FactoryBean#getObject()
     */
	public DefaultBootstrapProducer getObject() throws Exception {
		BootstrapProducerConfig producerConfig = new BootstrapProducerConfig();
		ConfigLoader<BootstrapProducerStaticConfig> staticProducerConfigLoader = new ConfigLoader<BootstrapProducerStaticConfig>(
				BootstrapConfig.getPropertiesPrefix(), producerConfig);
		// create a merged properties list from Relay Client and Bootstrap specific properties
		Properties mergedProperties = this.bootstrapConfig.getRelayClientBootstrapProperties();
		mergedProperties.putAll(this.bootstrapConfig.getRelayClientProperties());		
		BootstrapProducerStaticConfig staticProducerConfig = staticProducerConfigLoader.loadConfig(mergedProperties);
	    return new DefaultBootstrapProducer(staticProducerConfig);	
	}

	/**
	 * Interface method implementation. Checks for mandatory dependencies and initializes this Relay Client Bootstrap producer
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.bootstrapConfig,"'bootstrapConfig' cannot be null. This Relay Client Bootstrap producer will not be initialized");
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
	public BootstrapConfig getBootstrapConfig() {
		return bootstrapConfig;
	}
	public void setBootstrapConfig(BootstrapConfig bootstrapConfig) {
		this.bootstrapConfig = bootstrapConfig;
	}
	
}

