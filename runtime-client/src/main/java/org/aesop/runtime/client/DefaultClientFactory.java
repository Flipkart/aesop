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
package org.aesop.runtime.client;

import org.aesop.runtime.config.ClientConfig;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * The Spring factory bean for creating {@link DefaultClient} instances based on configured properties
 * 
 * @author Regunath B
 * @version 1.0, 16 Jan 2014
 */
public class DefaultClientFactory  implements FactoryBean<DefaultClient>, InitializingBean {
	
	/** Constant for the Databus stats collector*/
	public static final String STATS_COLLECTOR = "statsCollector";
	
	/** The configuration details for creating the Relay Client*/
	private ClientConfig clientConfig;
	
    /**
     * Interface method implementation. Creates and returns a {@link DefaultClient} instance
     * @see org.springframework.beans.factory.FactoryBean#getObject()
     */
	public DefaultClient getObject() throws Exception {
		return null;
	}

	/**
	 * Interface method implementation. Checks for mandatory dependencies and initializes this Relay Client
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.clientConfig,"'clientConfig' cannot be null. This Relay Client will not be initialized");
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
}
