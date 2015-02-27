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

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.PlatformException;
import org.trpr.platform.runtime.common.RuntimeVariables;

/**
 * <code>ClientConfig</code> holds Databus configuration properties for a Databus Client instance. This config treats the
 * properties as opaque and is intended for use as a holder of the information.
 *
 * @author Regunath B
 * @version 1.0, 23 Jan 2014
 */

public class ClientConfig implements InitializingBean {

	/** The property name prefix for all Databus client properties*/
	protected static final String CLIENT_PROPERTIES_PREFIX = "databus.client.";
	
	/** Property names for referencing the Relay*/
	private static final String RELAY="runtime.relay";
	private static final String RELAY_HOST=".host";
	private static final String RELAY_PORT=".port";
	private static final String RELAY_LOGICAL_SOURCES=".sources";	

	/** Property names for referencing the Bootstrap*/
	private static final String BOOTSTRAP="runtime.bootstrap.service";
	private static final String BOOTSTRAP_HOST=".host";
	private static final String BOOTSTRAP_PORT=".port";
	private static final String BOOTSTRAP_LOGICAL_SOURCES=".sources";	
	
	/** The property to signal Bootstrap enabling*/
	private static final String BOOTSTRAP_ENABLED="runtime.bootstrap.enabled";
	
	/** The client checkpoint file location property name*/
	public static final String CHECKPOINT_DIR_PROPERTY = "checkpointPersistence.fileSystem.rootDirectory";
	
	/** The override for client properties prefix */
	private String clientPropertiesPrefix = ClientConfig.CLIENT_PROPERTIES_PREFIX;
	
	/** The Properties instance holding Databus Client configuration data*/
	private Properties clientProperties = new Properties();

	/** The client checkpoint directory location*/
	private String checkpointDirectoryLocation;
	
	/** List of Relay Client config instances*/
	private List<RelayClientConfig> relayClientConfigs;

	/** List of Bootstrap Client config instances*/
	private List<BootstrapClientConfig> bootstrapClientConfigs;
	
	/**
	 * Interface method implementation. Ensures that all property names start with {@link ClientConfig#getPropertiesPrefix()}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.relayClientConfigs,"'relayClientConfig' cannot be null. This Databus Client will not be initialized");		
		Assert.notNull(this.checkpointDirectoryLocation,"'checkpointDirectoryLocation' cannot be null. This Databus Client will not be initialized");		
		for (Object key : this.clientProperties.keySet()) {
			if (!((String)key).startsWith(getClientPropertiesPrefix())) {
				throw new PlatformException("Property : " + key + " does not begin with the prefix : " + getClientPropertiesPrefix());
			}
		}
	}
	
	/** Getter/Setter properties*/	
	public String getClientPropertiesPrefix() {
		return clientPropertiesPrefix;
	}
	public void setClientPropertiesPrefix(String clientPropertiesPrefix) {
		this.clientPropertiesPrefix = clientPropertiesPrefix;
	}
	public Properties getClientProperties() {
		return this.clientProperties;
	}
	public void setClientProperties(Properties clientProperties) {
		this.clientProperties = clientProperties;
	}
	public String getCheckpointDirectoryLocation() {
		return checkpointDirectoryLocation;
	}
	public void setCheckpointDirectoryLocation(String checkpointDirectoryLocation) {
		this.checkpointDirectoryLocation = checkpointDirectoryLocation;
		// add the checkpoint directory location to the properties specified for the Relay Client. 
		// The checkpoint directory is relative to projects root
		this.getClientProperties().put(getClientPropertiesPrefix() + CHECKPOINT_DIR_PROPERTY, 
				new File(RuntimeVariables.getProjectsRoot() + File.separator  + this.checkpointDirectoryLocation).getAbsolutePath());				
	}	
	public List<RelayClientConfig> getRelayClientConfig() {
		return relayClientConfigs;
	}
	public void setRelayClientConfigs(List<RelayClientConfig> relayClientConfigs) {
		this.relayClientConfigs = relayClientConfigs;
		for(RelayClientConfig relayClientConfig : this.relayClientConfigs)
		{
			// add the relay host to the properties specified for the Relay Client. 
			this.getClientProperties().put(getClientPropertiesPrefix() + this.getRelayProperty(relayClientConfig) + RELAY_HOST, relayClientConfig.getRelayHost());
			// add the relay port to the properties specified for the Relay Client. 
			this.getClientProperties().put(getClientPropertiesPrefix() + this.getRelayProperty(relayClientConfig) + RELAY_PORT, relayClientConfig.getRelayPort());
			// add the relay logical source name to the properties specified for the Relay Client.
			String commaSeparedLogicalSources = StringUtils.join(relayClientConfig.getRelayLogicalSourceNames(), ",");
			this.getClientProperties().put(getClientPropertiesPrefix() + this.getRelayProperty(relayClientConfig) + RELAY_LOGICAL_SOURCES, commaSeparedLogicalSources);
		}
	}	
	public List<BootstrapClientConfig> getBootstrapClientConfig() {
		return bootstrapClientConfigs;
	}
	public void setBootstrapClientConfig(List<BootstrapClientConfig> bootstrapClientConfigs) {
		this.bootstrapClientConfigs = bootstrapClientConfigs;
		for(BootstrapClientConfig bootstrapClientConfig : bootstrapClientConfigs)
		{
			// add the bootstrap host to the properties specified for the Bootstrap Client. 
			this.getClientProperties().put(getClientPropertiesPrefix() + this.getBootstrapProperty(bootstrapClientConfig) + BOOTSTRAP_HOST, bootstrapClientConfig.getBootstrapHost());
			// add the bootstrap port to the properties specified for the Bootstrap Client. 
			this.getClientProperties().put(getClientPropertiesPrefix() + this.getBootstrapProperty(bootstrapClientConfig) + BOOTSTRAP_PORT, bootstrapClientConfig.getBootstrapPort());
			// add the bootstrap logical source name to the properties specified for the Bootstrap Client.
			String commaSeparedLogicalSources = StringUtils.join(bootstrapClientConfig.getBootstrapLogicalSourceNames(), ",");
			this.getClientProperties().put(getClientPropertiesPrefix() + this.getBootstrapProperty(bootstrapClientConfig) + BOOTSTRAP_LOGICAL_SOURCES, commaSeparedLogicalSources);
			// add property to indicate that Bootstrapping is enabled for the client
			this.getClientProperties().put(getClientPropertiesPrefix() + BOOTSTRAP_ENABLED, true);
		}
	}

	/**
	 * Helper method to get the Relay property appended with the relay ID
	 */
	private String getRelayProperty(RelayClientConfig relayClientConfig) {
		return RELAY + "(" + relayClientConfig.getRelayId() + ")";
	}
	/**
	 * Helper method to get the Bootstrap property appended with the bootstrap ID
	 */
	private String getBootstrapProperty(BootstrapClientConfig bootstrapClientConfig) {
		return BOOTSTRAP + "(" + bootstrapClientConfig.getBootstrapId() + ")";
	}
}
