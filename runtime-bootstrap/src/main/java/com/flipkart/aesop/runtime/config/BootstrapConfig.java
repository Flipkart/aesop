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

import org.springframework.util.Assert;
import org.trpr.platform.core.PlatformException;
import org.trpr.platform.runtime.common.RuntimeVariables;

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
	
	/** The bootstrap server checkpoint file location property name*/
	public static final String CHECKPOINT_DIR_PROPERTY = "db.client.checkpointPersistence.fileSystem.rootDirectory";
	
	/** The Properties instance holding Databus Bootstrap configuration data*/
	private Properties bootstrapProperties = new Properties();

	/** The client checkpoint directory location*/
	private String checkpointDirectoryLocation;
	
	/**
	 * Interface method implementation. Ensures that all property names start with {@link BootstrapConfig#BOOTSTRAP_PROPERTIES_PREFIX}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.checkpointDirectoryLocation,"'checkpointDirectoryLocation' cannot be null. This Bootstrap Server will not be initialized");		
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
	public String getCheckpointDirectoryLocation() {
		return checkpointDirectoryLocation;
	}
	public void setCheckpointDirectoryLocation(String checkpointDirectoryLocation) {
		this.checkpointDirectoryLocation = checkpointDirectoryLocation;
		// add the checkpoint directory location to the properties specified for the Bootstrap Server. 
		// The checkpoint directory is relative to projects root
		this.getBootstrapProperties().put(BootstrapConfig.BOOTSTRAP_PROPERTIES_PREFIX + CHECKPOINT_DIR_PROPERTY, 
				new File(RuntimeVariables.getProjectsRoot() + File.separator  + this.checkpointDirectoryLocation).getAbsolutePath());				
	}

}
