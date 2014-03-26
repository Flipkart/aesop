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

package com.flipkart.aesop.runtime.spi.admin;

import java.io.File;
import java.util.List;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.trpr.platform.core.PlatformException;

import com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry;
import com.linkedin.databus2.core.container.netty.ServerContainer;


/**
 * <code>RuntimeConfigService</code> provides methods for managing runtime configurations
 *
 * @author Regunath B
 * @version 1.0, 05 Jan 2014
 */
public interface RuntimeConfigService {

    /**
     * Add an {@link AbstractRuntimeRegistry} to the list of runtime registries
     * @param registry The {@link AbstractRuntimeRegistry} instance
     */
    public void addRuntimeRegistry(AbstractRuntimeRegistry registry);

	/**
	 * Gets the runtime configuration file as a resource
	 * @param runtimeName Name of the runtime
	 * @return Configuration file
	 */
	public Resource getRuntimeConfig(String runtimeName);
	
	/**
	 * Modifies the config file for the given runtime 
	 * @param modifiedRuntimeConfigFile This will be set as the runtime configuration file for all runtime instances present in the File
	 * @throws PlatformException in case of inconsistencies
	 */
	public void modifyRuntimeConfig(String runtimeName, ByteArrayResource modifiedRuntimeConfigFile) throws PlatformException;

	/**
	 * Method to inject runtime file name 
	 */
	public void addRuntimeConfigPath(File runtimeFile, ServerContainer runtime);

    /**
     * Re-initializes a runtime, if found. Calls the destroy() and init() methods.
     * @param runtimeName the name of the runtime to be re-inited
     */
    public void reinitRuntime(String runtimeName) throws Exception;

    /**
     * Gets all runtime instances
     */
    public List<ServerContainer> getAllRuntimes();

}
