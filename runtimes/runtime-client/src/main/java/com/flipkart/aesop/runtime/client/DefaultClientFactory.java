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
package com.flipkart.aesop.runtime.client;

import com.flipkart.aesop.runtime.config.ClientConfig;
import com.flipkart.aesop.runtime.config.ConsumerRegistration;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.core.util.ConfigLoader;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * The Spring factory bean for creating {@link DefaultClient} instances based on configured properties
 * 
 * @author Regunath B
 * @version 1.0, 16 Jan 2014
 */
public class DefaultClientFactory implements FactoryBean<DefaultClient>, InitializingBean {
	
	/** The configuration details for creating the Relay Client*/
	private ClientConfig clientConfig;
	
	/** The ConsumerRegistration list for the Relay Client*/
	private List<ConsumerRegistration> consumerRegistrationList = new ArrayList<ConsumerRegistration>();
	
    /**
     * Interface method implementation. Creates and returns a {@link DefaultClient} instance
     * @see org.springframework.beans.factory.FactoryBean#getObject()
     */
	public DefaultClient getObject() throws Exception {
		DatabusHttpClientImpl.Config config = new DatabusHttpClientImpl.Config();		
		ConfigLoader<DatabusHttpClientImpl.StaticConfig> staticConfigLoader = new ConfigLoader<DatabusHttpClientImpl.StaticConfig>(clientConfig.getClientPropertiesPrefix(), config);
		DatabusHttpClientImpl.StaticConfig staticConfig = staticConfigLoader.loadConfig(this.clientConfig.getClientProperties());
		DefaultClient defaultClient = new DefaultClient(staticConfig);
		// register all Event Consumers with the Relay Client
		for (ConsumerRegistration consumerRegistration : this.consumerRegistrationList) {
			defaultClient.registerDatabusStreamListener(consumerRegistration.getEventConsumer(), null, 
					consumerRegistration.getLogicalSources().toArray(new String[0]));
			// add the bootstrap consumer only if a bootstrap config as been set
			if (consumerRegistration.isReadFromBootstrap()) {
				defaultClient.registerDatabusBootstrapListener(consumerRegistration.getEventConsumer(), null, 
						consumerRegistration.getLogicalSources().toArray(new String[0]));
			}
		}
	    return defaultClient;	
	}

	/**
	 * Interface method implementation. Checks for mandatory dependencies and initializes this Relay Client
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.clientConfig,"'clientConfig' cannot be null. This Relay Client will not be initialized");
		Assert.notEmpty(this.consumerRegistrationList,"'consumerRegistrationList' cannot be empty. No Event consumers registered");		
	}
	
	/**
	 * Interface method implementation. Returns the DefaultClient type
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<DefaultClient> getObjectType() {
		return DefaultClient.class;
	}

	/**
	 * Interface method implementation. Returns true
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */	
	public boolean isSingleton() {
		return true;
	}

	/** Getter/Setter methods to override default implementations of various components used by this Relay Client*/
	public ClientConfig getClientConfig() {
		return this.clientConfig;
	}
	public void setClientConfig(ClientConfig clientConfig) {
		this.clientConfig = clientConfig;
	}
	public List<ConsumerRegistration> getConsumerRegistrationList() {
		return consumerRegistrationList;
	}
	public void setConsumerRegistrationList(List<ConsumerRegistration> consumerRegistrationList) {
		this.consumerRegistrationList = consumerRegistrationList;
	}
	
}
