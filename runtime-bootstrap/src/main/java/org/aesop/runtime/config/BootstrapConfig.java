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
package org.aesop.runtime.config;

import java.util.Properties;

import org.trpr.platform.core.PlatformException;

/**
 * <code>BootstrapConfig</code> holds Databus configuration properties for a Bootstrap server that serves change data snapshots. 
 * This config treats the properties as opaque and is intended for use as a holder of the information.
 *
 * @author Regunath B
 * @version 1.0, 10 Feb 2014
 */
public class BootstrapConfig {

	/** The property name prefix for all Databus bootstrap properties*/
	public static final String BOOTSTRAP_PROPERTIES_PREFIX = "databus.bootstrap.";
	
	/** The Properties instance holding Databus Bootstrap configuration data*/
	private Properties bootstrapProperties = new Properties();

	/**
	 * Interface method implementation. Ensures that all property names start with {@link BootstrapConfig#BOOTSTRAP_PROPERTIES_PREFIX}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		for (Object key : this.bootstrapProperties.keySet()) {
			if (!((String)key).startsWith(BootstrapConfig.BOOTSTRAP_PROPERTIES_PREFIX)) {
				throw new PlatformException("Property : " + key + " does not begin with the prefix : " + BootstrapConfig.BOOTSTRAP_PROPERTIES_PREFIX);
			}
		}
	}

	/** Getter/Setter properties*/
	public Properties getBootstrapProperties() {
		return bootstrapProperties;
	}
	public void setBootstrapProperties(Properties bootstrapProperties) {
		this.bootstrapProperties = bootstrapProperties;
	}	

}
