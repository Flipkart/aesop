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
package com.flipkart.aesop.runtime.producer.impl;

import java.io.File;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.runtime.common.RuntimeVariables;

import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.FileSystemCheckpointPersistenceProvider;

/**
 * The Spring factory bean for creating file based {@link CheckpointPersistenceProvider}. Used by relay producer implementations for 
 * creating {@link SCNGenerator} instances.
 * 
 * @author Regunath B
 * @version 1.0, 30 Mar 2015
 */

public class FileSystemCPProviderFactory implements FactoryBean<CheckpointPersistenceProvider>, InitializingBean {
	
	/** Properties for the file based checkpoint provider*/
	private static final int DEFAULT_PROTOCOL_VERSION = 2;	
	private String checkpointDir;
	private int maxHistorySize = FileSystemCheckpointPersistenceProvider.RuntimeConfigBuilder.MAX_HISTORY_SIZE;

    /**
     * Interface method implementation. Creates and returns a {@link CheckpointPersistenceProvider} instance
     * @see org.springframework.beans.factory.FactoryBean#getObject()
     */
	public CheckpointPersistenceProvider getObject() throws Exception {
		FileSystemCheckpointPersistenceProvider.Config config = new FileSystemCheckpointPersistenceProvider.Config();
	    config.setRootDirectory(new File(RuntimeVariables.getProjectsRoot() + File.separator + this.checkpointDir).getAbsolutePath());
	    config.getRuntime().setHistoryEnabled(true);
	    config.getRuntime().setHistorySize(this.maxHistorySize);
	    FileSystemCheckpointPersistenceProvider checkpointProvider = new FileSystemCheckpointPersistenceProvider(config, DEFAULT_PROTOCOL_VERSION);		 
		return checkpointProvider;
	}
	
	/**
	 * Interface method implementation. Returns the CheckpointPersistenceProvider type
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<CheckpointPersistenceProvider> getObjectType() {
		return CheckpointPersistenceProvider.class;
	}

	/**
	 * Interface method implementation. Returns true
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */	
	public boolean isSingleton() {
		return true;
	}
	
	/**
	 * Interface method implementation. Checks for mandatory dependencies 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.checkpointDir,"'checkpointDir' cannot be null. This file based checkpoint provider will not be initialized");
	}

	/** Setter/Getter methods */
	public String getCheckpointDir() {
		return checkpointDir;
	}
	public void setCheckpointDir(String checkpointDir) {
		this.checkpointDir = checkpointDir;
	}
	public int getMaxHistorySize() {
		return maxHistorySize;
	}
	public void setMaxHistorySize(int maxHistorySize) {
		this.maxHistorySize = maxHistorySize;
	}	
	
}
