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

package com.flipkart.aesop.runtime.spring.registry;

import java.io.File;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.flipkart.aesop.runtime.spring.RuntimeComponentContainer;

/**
 * The <code>ServerContainerConfigInfo</code> class is a structure that holds runtime ServerContainer configuration information and ApplicationContext for  
 * managing the life-cycle of the runtime
 * 
 * @author Regunath B
 * @version 1.0, 03 Jan 2014
 */
public class ServerContainerConfigInfo {

	/** The sub-folder containing runtime and dependent binaries. This is used in addition to the runtime classpath.
	 *  This path is relative to the location where {@link RuntimeComponentContainer#getRuntimeConfigFileName()} file is found 
	 */
	public static final String BINARIES_PATH = "lib";

	/** The prefix to be added to file absolute paths when loading Spring XMLs using the FileSystemXmlApplicationContext*/
	public static final String FILE_PREFIX = "file:";
	
	/** The the RuntimeComponentContainer.getRuntimeConfigFileName() file containing runtime bean */
	private File xmlConfigFile;
	
	/** The path to runtime and dependent binaries*/
	private String binariesPath = ServerContainerConfigInfo.BINARIES_PATH;
	
	/** The Spring ApplicationContext initialized using information contained in this ServerContainerConfigInfo*/
	private AbstractApplicationContext runtimeContext;
	
	/**
	 * Constructors
	 */
	public ServerContainerConfigInfo(File xmlConfigFile) {
		this.xmlConfigFile = xmlConfigFile;
	}
	public ServerContainerConfigInfo(File xmlConfigFile, String binariesPath) {
		this(xmlConfigFile);
		this.binariesPath = binariesPath;
	}
	public ServerContainerConfigInfo(File xmlConfigFile, String binariesPath, AbstractApplicationContext runtimeContext) {
		this(xmlConfigFile,binariesPath);
		this.runtimeContext = runtimeContext;
	}

	/**
	 * Loads and returns an AbstractApplicationContext using data contained in this class
	 * @return the runtime's AbstractApplicationContext
	 */
	public AbstractApplicationContext loadRuntimeContext(ClassLoader classLoader, AbstractApplicationContext applicationContext) {
		ClassLoader existingTCCL = Thread.currentThread().getContextClassLoader();
		// set the custom classloader as the tccl for loading the runtime
		Thread.currentThread().setContextClassLoader(classLoader);
		// add the "file:" prefix to file names to get around strange behavior of FileSystemXmlApplicationContext that converts absolute path 
		// to relative path
		this.runtimeContext = new FileSystemXmlApplicationContext(
                new String[]{FILE_PREFIX + this.xmlConfigFile.getAbsolutePath()},
				applicationContext
        );
		// now reset the thread's TCCL to the one that existed prior to loading the runtime
		Thread.currentThread().setContextClassLoader(existingTCCL);
		return this.runtimeContext;
	}

	/**
	 * Overriden super type method. Returns true if the path to the runtime context is the same i.e. loaded from the same file
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object object) {
		ServerContainerConfigInfo otherConfigInfo = (ServerContainerConfigInfo)object;
		return this.getXmlConfigFile().getAbsolutePath().equalsIgnoreCase(otherConfigInfo.getXmlConfigFile().getAbsolutePath());
	}
	
	/**
	 * Overriden superclass method. Prints the xmlConfigFile details
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return  "ServerContainerConfigInfo [xmlConfigFile=" + xmlConfigFile + ", binariesPath=" + binariesPath + "]";
	}
	
	/** Getter methods*/
	/**
	 * Returns the runtime's ApplicationContext, if loaded, else null
	 * @return null or the runtime's AbstractApplicationContext
	 */
	public AbstractApplicationContext getRuntimeContext() {
		return this.runtimeContext;
	}
	public File getXmlConfigFile() {
		return this.xmlConfigFile;
	}
	public String getBinariesPath() {
		return this.binariesPath;
	}
	
}
