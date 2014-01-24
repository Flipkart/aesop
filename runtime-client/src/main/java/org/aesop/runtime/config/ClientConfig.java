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

import java.io.File;
import java.util.Properties;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.PlatformException;
import org.trpr.platform.runtime.common.RuntimeVariables;

/**
 * <code>ClientConfig</code> holds Databus configuration properties for a Relay Client instance. This config treats the
 * properties as opaque and is intended for use as a holder of the information.
 *
 * @author Regunath B
 * @version 1.0, 23 Jan 2014
 */

public class ClientConfig implements InitializingBean {

	/** The property name prefix for all Databus relay client properties*/
	public static final String CLIENT_PROPERTIES_PREFIX = "databus.client.";
	
	/** The client checkpoint file location property name*/
	public static final String CHECKPOINT_DIR_PROPERTY = "checkpointPersistence.fileSystem.rootDirectory";
	
	/** The identifier for the Relay that the Relay Client is connecting to*/
	private String relayId;
	
	/** The Logical Source name in the Relay that the Relay Client will consume change events from */
	private String relayLogicalSourceName;
	
	/** The Properties instance holding Relay Client configuration data*/
	private Properties relayClientProperties = new Properties();

	/** The client checkpoint directory location*/
	private String checkpointDirectoryLocation;
		
	/**
	 * Interface method implementation. Ensures that all property names start with {@link ClientConfig#CLIENT_PROPERTIES_PREFIX}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.checkpointDirectoryLocation,"'checkpointDirectoryLocation' cannot be null. This Relay Client will not be initialized");		
		Assert.notNull(this.relayId,"'relayId' cannot be null. This Relay Client will not be initialized");		
		Assert.notNull(this.relayLogicalSourceName,"'relayLogicalSourceName' cannot be null. This Relay Client will not be initialized");		
		for (Object key : this.relayClientProperties.keySet()) {
			if (!((String)key).startsWith(ClientConfig.CLIENT_PROPERTIES_PREFIX)) {
				throw new PlatformException("Property : " + key + " does not begin with the prefix : " + ClientConfig.CLIENT_PROPERTIES_PREFIX);
			}
		}
	}
	
	/** Getter/Setter properties*/
	public Properties getRelayClientProperties() {
		return this.relayClientProperties;
	}
	public void setRelayClientProperties(Properties relayClientProperties) {
		this.relayClientProperties = relayClientProperties;
	}
	public String getCheckpointDirectoryLocation() {
		return checkpointDirectoryLocation;
	}
	public void setCheckpointDirectoryLocation(String checkpointDirectoryLocation) {
		this.checkpointDirectoryLocation = checkpointDirectoryLocation;
		// add the checkpoint directory location to the properties specified for the Relay Cient. 
		// The checkpoint directory is relative to projects root
		this.getRelayClientProperties().put(CLIENT_PROPERTIES_PREFIX + CHECKPOINT_DIR_PROPERTY, 
				new File(RuntimeVariables.getProjectsRoot() + File.separator  + this.checkpointDirectoryLocation).getAbsolutePath());				
	}
	public String getRelayId() {
		return relayId;
	}
	public void setRelayId(String relayId) {
		this.relayId = relayId;
	}
	public String getRelayLogicalSourceName() {
		return relayLogicalSourceName;
	}
	public void setRelayLogicalSourceName(String relayLogicalSourceName) {
		this.relayLogicalSourceName = relayLogicalSourceName;
	}	
}
