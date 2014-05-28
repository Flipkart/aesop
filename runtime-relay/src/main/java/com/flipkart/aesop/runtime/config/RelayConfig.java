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
import java.util.Properties;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.PlatformException;
import org.trpr.platform.runtime.common.RuntimeVariables;
import org.trpr.platform.runtime.impl.config.FileLocator;

/**
 * <code>RelayConfig</code> holds Databus configuration properties for a Relay
 * instance. This config treats the properties as opaque and is intended for use
 * as a holder of the information.
 * 
 * @author Regunath B
 * @version 1.0, 15 Jan 2014
 */

public class RelayConfig implements InitializingBean {

	/** The property name prefix for all Databus relay properties */
	public static final String RELAY_PROPERTIES_PREFIX = "databus.relay.";

	/** The Memory mapped file directory property name */
	public static final String MMAPPED_DIR_PROPERTY = "eventBuffer.mmapDirectory";

	/** The MaxSCN information file location property name */
	public static final String MAXSCN_DIR_PROPERTY = "dataSources.sequenceNumbersHandler.file.scnDir";

	/** The Properties instance holding Relay configuration data */
	private Properties relayProperties = new Properties();

	/** The schemas registry location */
	private String schemaRegistryLocation;

	/** The Memory mapped event buffer directory location */
	private String mmappedDirectoryLocation;

	/** The MAX SCN file directory location */
	private String maxScnDirectoryLocation;

	/**
	 * Interface method implementation. Ensures that all property names start
	 * with {@link RelayConfig#RELAY_PROPERTIES_PREFIX}
	 * 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.schemaRegistryLocation,
				"'schemaRegistryLocation' cannot be null. This Relay will not be initialized");
		Assert.notNull(this.mmappedDirectoryLocation,
				"'mmappedDirectoryLocation' cannot be null. This Relay will not be initialized");
		Assert.notNull(this.maxScnDirectoryLocation,
				"'maxScnDirectoryLocation' cannot be null. This Relay will not be initialized");
		for (Object key : this.relayProperties.keySet()) {
			if (!((String) key).startsWith(RelayConfig.RELAY_PROPERTIES_PREFIX)) {
				throw new PlatformException("Property : " + key
						+ " does not begin with the prefix : "
						+ RelayConfig.RELAY_PROPERTIES_PREFIX);
			}
		}
	}

	/** Getter/Setter properties */
	public Properties getRelayProperties() {
		return this.relayProperties;
	}
	public void setRelayProperties(Properties relayProperties) {
		this.relayProperties = relayProperties;
	}
	public String getSchemaRegistryLocation() {
		return schemaRegistryLocation;
	}
	public void setSchemaRegistryLocation(String schemaRegistryLocation) {
		File[] foundFiles = FileLocator.findDirectories(schemaRegistryLocation,
				null);
		if (foundFiles.length > 0) {
			this.schemaRegistryLocation = foundFiles[0].getAbsolutePath();
		} else {
			this.schemaRegistryLocation = schemaRegistryLocation;
		}
	}
	public String getMmappedDirectoryLocation() {
		return mmappedDirectoryLocation;
	}
	public void setMmappedDirectoryLocation(String mmappedDirectoryLocation) {
		this.mmappedDirectoryLocation = mmappedDirectoryLocation;
		// add the Memory mapped file directory location to the properties
		// specified for the Relay.
		// The MMapped directory is relative to projects root
		this.getRelayProperties().put(
				RELAY_PROPERTIES_PREFIX + MMAPPED_DIR_PROPERTY,
				new File(RuntimeVariables.getProjectsRoot() + File.separator
						+ this.mmappedDirectoryLocation).getAbsolutePath());
	}
	public String getMaxScnDirectoryLocation() {
		return maxScnDirectoryLocation;
	}
	public void setMaxScnDirectoryLocation(String maxScnDirectoryLocation) {
		this.maxScnDirectoryLocation = maxScnDirectoryLocation;
		// add the Max SCN file directory location to the properties specified
		// for the Relay.
		// The Max SCN directory is relative to projects root
		this.getRelayProperties().put(
				RELAY_PROPERTIES_PREFIX + MAXSCN_DIR_PROPERTY,
				new File(RuntimeVariables.getProjectsRoot() + File.separator
						+ this.maxScnDirectoryLocation).getAbsolutePath());
	}

}
