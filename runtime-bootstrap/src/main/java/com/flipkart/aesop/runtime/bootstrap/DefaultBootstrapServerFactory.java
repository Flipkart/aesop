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

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.aesop.runtime.config.BootstrapConfig;
import com.linkedin.databus.bootstrap.server.BootstrapServerConfig;
import com.linkedin.databus.bootstrap.server.BootstrapServerStaticConfig;
import com.linkedin.databus.core.util.ConfigLoader;

/**
 * The Spring factory bean for creating {@link DefaultBootstrapServer} instances based on configured properties
 * 
 * @author Regunath B
 * @version 1.0, 12 Feb 2014
 */
public class DefaultBootstrapServerFactory implements FactoryBean<DefaultBootstrapServer>, InitializingBean {
	
	/** The configuration details for creating the Bootstrap server */
	private BootstrapConfig bootstrapConfig;
	
    /**
     * Interface method implementation. Creates and returns a {@link DefaultBootstrapServer} instance
     * @see org.springframework.beans.factory.FactoryBean#getObject()
     */
	public DefaultBootstrapServer getObject() throws Exception {
		BootstrapServerConfig config = new BootstrapServerConfig();
		ConfigLoader<BootstrapServerStaticConfig> configLoader = new ConfigLoader<BootstrapServerStaticConfig>(BootstrapConfig.BOOTSTRAP_PROPERTIES_PREFIX, config);
		BootstrapServerStaticConfig staticConfig = configLoader.loadConfig(this.bootstrapConfig.getBootstrapProperties());
		DefaultBootstrapServer bootstrapServer = new DefaultBootstrapServer(staticConfig);
		return bootstrapServer;
	}

	/**
	 * Interface method implementation. Checks for mandatory dependencies 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.bootstrapConfig,"'bootstrapConfig' cannot be null. This Bootstrap server will not be initialized");
	}
	
	/**
	 * Interface method implementation. Returns the DefaultBootstrapServer type
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<DefaultBootstrapServer> getObjectType() {
		return DefaultBootstrapServer.class;
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

