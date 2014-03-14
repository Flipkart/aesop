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
package com.flipkart.aesop.runtime.config;

import java.util.Properties;

import org.trpr.platform.core.PlatformException;

/**
 * <code>BootstrapProducerConfig</code> holds Databus configuration properties for a Relay Client instance that acts as a Bootstrap
 * data producer. This config treats the properties as opaque and is intended for use as a holder of the information.
 *
 * @author Regunath B
 * @version 1.0, 06 Feb 2014
 */
public class BootstrapProducerConfig {

	/** The property name prefix for all Databus relay client bootstrap properties*/
	public static final String BOOTSTRAP_PROPERTIES_PREFIX = "databus.bootstrap.";
	
	/** The Properties instance holding Relay Client Bootstrap configuration data*/
	private Properties relayClientBootstrapProperties = new Properties();

	/**
	 * Interface method implementation. Ensures that all property names start with {@link BootstrapProducerConfig#BOOTSTRAP_PROPERTIES_PREFIX}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		for (Object key : this.relayClientBootstrapProperties.keySet()) {
			if (!((String)key).startsWith(BootstrapProducerConfig.BOOTSTRAP_PROPERTIES_PREFIX)) {
				throw new PlatformException("Property : " + key + " does not begin with the prefix : " + BootstrapProducerConfig.BOOTSTRAP_PROPERTIES_PREFIX);
			}
		}
	}
	
	/** Getter/Setter properties*/
	public Properties getRelayClientBootstrapProperties() {
		return relayClientBootstrapProperties;
	}
	public void setRelayClientBootstrapProperties(Properties relayClientBootstrapProperties) {
		this.relayClientBootstrapProperties = relayClientBootstrapProperties;
	}

}
