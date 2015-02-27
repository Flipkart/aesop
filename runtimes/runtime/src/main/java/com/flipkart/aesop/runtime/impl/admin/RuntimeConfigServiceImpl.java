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
package com.flipkart.aesop.runtime.impl.admin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.trpr.platform.core.PlatformException;

import com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService;
import com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry;
import com.flipkart.aesop.runtime.spring.RuntimeComponentContainer;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * <code>RuntimeConfigServiceImpl</code> is an implementation of {@link RuntimeConfigService}.
 * 
 * @author Regunath B
 * @version 1.0, 06 Jan, 2014
 */
public class RuntimeConfigServiceImpl  implements RuntimeConfigService {

	/** Logger instance for this class*/
	private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeConfigServiceImpl.class);
	
	/** The previous runtime file (save file) name prefix*/
	public static final String PREV_RUNTIME_FILE_PREFIX = "prev-";
	
	/** The ComponentContainer instance for reloading the config file */
	private RuntimeComponentContainer componentContainer;
	
	/** The map holding the mapping of a config file to it's runtime name */
	private Map<URI, List<ServerContainer> > configURItoRuntimeName = new HashMap<URI,List<ServerContainer>>();
	
    /** List of repositories */
    private List<AbstractRuntimeRegistry> registries = new ArrayList<AbstractRuntimeRegistry>();

    /**
     * Interface method implementation
     * @see com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService#addRuntimeRegistry(com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry)
     */
	public void addRuntimeRegistry(AbstractRuntimeRegistry registry) {
		this.registries.add(registry);
	}

	/**
	 * Interface method implementation. Returns null if the Resource is not found
	 * @see com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService#getRuntimeConfig(java.lang.String)
	 */
	public Resource getRuntimeConfig(String runtimeName) {
        for (URI configFile: this.configURItoRuntimeName.keySet()) {
            for (ServerContainer runtime : this.configURItoRuntimeName.get(configFile)) {
                if (runtime.getComponentAdmin().getComponentName().equals(runtimeName)) {
                    return new FileSystemResource(new File(configFile));
                }
            }
        }
        return null;
	}

	/**
	 * Interface method implementation.
	 * @see com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService#modifyRuntimeConfig(java.lang.String, org.springframework.core.io.ByteArrayResource)
	 */
	public void modifyRuntimeConfig(String runtimeName, ByteArrayResource modifiedRuntimeConfigFile) throws PlatformException {
    	// Check if exists
    	File oldRuntimeFile = null;
    	try {
    		oldRuntimeFile = this.getRuntimeConfig(runtimeName).getFile();
    	} catch (IOException e1) {
    		LOGGER.error("Config File for runtime: "+runtimeName+" not found. Returning");
    		throw new PlatformException("File not found for runtime: "+runtimeName,e1);
    	}
    	if (!oldRuntimeFile.exists()) {
    		LOGGER.error("Runtime Config File: "+oldRuntimeFile.getAbsolutePath()+" doesn't exist. Returning");
    		throw new PlatformException("File not found: "+oldRuntimeFile.getAbsolutePath());
    	}

        // Check for read permissions
    	if (!oldRuntimeFile.canRead()) {
    		LOGGER.error("No read permission for: "+oldRuntimeFile.getAbsolutePath()+". Returning");
    		throw new PlatformException("Read permissions not found for file: "+oldRuntimeFile.getAbsolutePath());
    	}

    	// Check for write permissions
    	if (!oldRuntimeFile.canWrite()) {
    		LOGGER.error("No write permission for: "+oldRuntimeFile.getAbsolutePath()+". Write permissions are required to modify runtime config");
    		throw new PlatformException("Required permissions not found for modifying file: "+oldRuntimeFile.getAbsolutePath());
    	}

        // create backup
    	this.createPrevConfigFile(runtimeName);

        // file_put_contents Java :-/
    	try {
    		this.upload(modifiedRuntimeConfigFile.getByteArray(), oldRuntimeFile.getAbsolutePath());
    	} catch (IOException e) {
    		LOGGER.error("IOException while uploading file to path: "+oldRuntimeFile.getAbsolutePath());
    		this.restorePrevConfigFile(runtimeName);
    		throw new PlatformException(e);
    	}

    	// get the registered ServerContainer for the file name
    	ServerContainer runtime = this.configURItoRuntimeName.get(oldRuntimeFile.toURI()).get(0);
        // re-load the runtime
        // TODO
        // loading component destroys all beans in the config file given by the runtime config info
        // this mean this only works if only one runtime per runtime xml file
    	try {
    		this.componentContainer.reloadRuntime(runtime, this.getRuntimeConfig(runtimeName));
    	} catch(Exception e) {
    		this.restorePrevConfigFile(runtimeName);
    		this.componentContainer.loadComponent(this.getRuntimeConfig(runtimeName));
    		throw new PlatformException(e);
    	}
    	this.removePrevConfigFile(runtimeName);
    }

	/**
	 * Interface method implementation
	 * @see com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService#addRuntimeConfigPath(java.io.File, com.linkedin.databus2.core.container.netty.ServerContainer)
	 */
	public void addRuntimeConfigPath(File runtimeFile, ServerContainer runtime) {
		if (this.configURItoRuntimeName.get(runtimeFile.toURI()) == null) {
			this.configURItoRuntimeName.put(runtimeFile.toURI(), new LinkedList<ServerContainer>());
		}
		this.configURItoRuntimeName.get(runtimeFile.toURI()).add(runtime);		
	}

	/**
	 * Interface method implementation.
	 * @see com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService#reinitRuntime(java.lang.String)
	 */
	public void reinitRuntime(String runtimeName) throws Exception {
        for (AbstractRuntimeRegistry registry : this.registries) {
            if (registry.getRuntime(runtimeName) != null) {
                registry.reinitRuntime(runtimeName);
            }
        }
	}

	/**
	 * Interface method implementation.
	 * @see com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService#getAllRuntimes()
	 */
	public List<ServerContainer> getAllRuntimes() {
        List<ServerContainer> list = new ArrayList<ServerContainer>();
        for (AbstractRuntimeRegistry registry : this.registries) {
            list.addAll(registry.getRuntimes());
        }
        return list;
	}
    
	/**
	 * Creates a temporary file, which is a duplicate of the current config file,
	 * with the name prefixed with {@link RuntimeConfigServiceImpl#PREV_RUNTIME_FILE_PREFIX}
	 * @param runtimeName name of the runtime
	 */
	private void createPrevConfigFile(String runtimeName) {
        // get current file
		File configFile = null;
		try {
			configFile = this.getRuntimeConfig(runtimeName).getFile();
		} catch (IOException e1) {
			LOGGER.error("Exception while getting runtime Config File",e1);
			throw new PlatformException("Exception while getting runtime Config File",e1);
		}

        // create the backup file
		File prevFile = new File(configFile.getParent()+"/"+RuntimeConfigServiceImpl.PREV_RUNTIME_FILE_PREFIX + configFile.getName());

        // move current -> backup, create new current file
		if (configFile.exists()) {
			if (prevFile.exists()) {
				prevFile.delete();
			}
			configFile.renameTo(prevFile);
			try {
				configFile.createNewFile();
			} catch (IOException e) {
				LOGGER.error("IOException while clearing config File",e);
				throw new PlatformException("IOException while clearing config File",e);
            }
			prevFile.deleteOnExit();
		}

	}

	/**
	 * This method removes the temporary previous config File
	 * @param runtimeName name of the runtime
	 */
	private void removePrevConfigFile(String runtimeName) {
		File configFile = null;
		try {
			configFile = this.getRuntimeConfig(runtimeName).getFile();
		} catch (IOException e) {
			LOGGER.error("IOException while getting runtime Config File",e);
		}
		String prevFilePath = configFile.getParent()+"/"+RuntimeConfigServiceImpl.PREV_RUNTIME_FILE_PREFIX + configFile.getName();
		File prevFile = new File(prevFilePath);
		if (prevFile.exists()) {
			prevFile.delete();  // DELETE previous XML File
		}
	}

	/**
	 * Restores the previous config file, if found
	 * @param runtimeName name of the runtime
	 */
	private void restorePrevConfigFile(String runtimeName) {
		File configFile = null;
		try {
			configFile = this.getRuntimeConfig(runtimeName).getFile();
		} catch (IOException e) {
			LOGGER.error("IOException while getting runtime config file",e);
		}
		if(configFile.exists()) {
			configFile.delete();
		}
		File prevFile = new File(configFile.getParent()+"/"+RuntimeConfigServiceImpl.PREV_RUNTIME_FILE_PREFIX + configFile.getName());;
		if(prevFile.exists()) {
			prevFile.renameTo(configFile);
		} 
	}

	/**
	 * Uploads the file to the given path. Creates the file and directory structure, if the file
	 * or parent directory doesn't exist
	 */
	private void upload(byte[] fileContents, String destPath) throws IOException {
		File destFile = new File(destPath);
		// If exists, overwrite
		if (destFile.exists()) {
			destFile.delete();
			destFile.createNewFile();
		}
		// Creating directory structure
		try {
			destFile.getParentFile().mkdirs();
		} catch(Exception e) {
			LOGGER.error("Unable to create directory structure for uploading file");
			throw new PlatformException("Unable to create directory structure for uploading file");
		}

		FileOutputStream fos = new FileOutputStream(destFile);
		fos.write(fileContents);						
	}
	
    /** Getter/Setter methods */
	public RuntimeComponentContainer getComponentContainer() {
		return componentContainer;
	}
	public void setComponentContainer(RuntimeComponentContainer componentContainer) {
		this.componentContainer = componentContainer;
	}
	/** End Getter/Setter methods */
}
