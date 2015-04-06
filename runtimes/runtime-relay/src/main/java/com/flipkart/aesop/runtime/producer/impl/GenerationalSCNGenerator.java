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

import java.io.IOException;
import java.util.Arrays;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusClientMode;

/**
 * <code>GenerationalSCNGenerator</code> is an implementation of the {@link SCNGenerator} that handles mastership change 
 * as monotonically increasing generation changes to create relay SCNs from local SCNs and host identifiers. 
 * This SCN generator works on the following assumptions/approach:
 * <pre>
 * 	<li>The local SCN has the format : high 32 bits derived from file location reference of local SCN, low 32 bits is the offset
 *      inside the file
 *  </li>
 *  <li>
 *      The generated relay SCN has the format : high 16 bits derived from generation, next 16 bits derived from location reference of
 *      local SCN, low 32 bits is the offset inside the file
 *  </li>
 * <pre>
 * @author Regunath B
 * @version 1.0, 25 Mar 2015
 */
public class GenerationalSCNGenerator implements SCNGenerator,InitializingBean {
	
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(GenerationalSCNGenerator.class);
	
	/** Invariants for no current generation and host*/
	private static final int NO_GENERATION = -1;
	private static final String NO_HOST = "";
	
	/** The current generation*/
	private int currentGeneration = NO_GENERATION;	
	/** The current host identifier*/
	private String currentHostId = NO_HOST;	
	
	/** The checkpoint persistence provider*/
	private CheckpointPersistenceProvider checkpointPersistenceProvider;
	
	/** The logical source name to identify the event stream*/
	private String relayLogicalSourceName;

	/**
	 * Interface method implementation. Checks for mandatory dependencies 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.relayLogicalSourceName,"'relayLogicalSourceName' cannot be null. This SCN generator will not be initialized");
		Assert.notNull(this.checkpointPersistenceProvider,"'checkpointPersistenceProvider' cannot be null. This SCN generator will not be initialized");
		Checkpoint cp = this.checkpointPersistenceProvider.loadCheckpoint(Arrays.asList(this.relayLogicalSourceName));
		if (cp != null) {
			this.currentGeneration = cp.getBootstrapSinceScn().intValue();
			this.currentHostId = cp.getBootstrapServerInfo();
		}
	}
	
	/**
	 * 
	 * @see com.flipkart.aesop.runtime.producer.SCNGenerator#getSCN(long, java.lang.String)
	 */
	public long getSCN(long localSCN, String hostId) {
		if (!hostId.equalsIgnoreCase(this.currentHostId) || this.currentGeneration == NO_GENERATION) {
			this.currentHostId = hostId;
			this.currentGeneration += 1;
			// we'll use a databus client checkpoint with appropriate(overloaded meaning) fields to store the generation and host name
			Checkpoint cp = new Checkpoint();
			cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			cp.setBootstrapSinceScn(Long.valueOf(this.currentGeneration));
			cp.setBootstrapServerInfo(this.currentHostId);
			cp.setBootstrapStartScn(localSCN);
			cp.setTsNsecs(System.nanoTime());
			try {
				this.checkpointPersistenceProvider.storeCheckpoint(Arrays.asList(this.relayLogicalSourceName),cp);
			} catch (IOException e) {
				LOGGER.error("Get SCN failed. Error storing checkpoint for : " + this.relayLogicalSourceName, e);
				// throw a Runtime exception so that the relay producer can handle it and stop generating more events
				throw new DatabusRuntimeException("Get SCN failed. Error storing checkpoint for : " + this.relayLogicalSourceName,e);
			}
		}
		long fileId = localSCN >> 32;
		long mask = (long)Integer.MAX_VALUE;
		long offset = localSCN & mask;			
		long scn = this.currentGeneration;
		scn <<= 8;
		scn |= fileId;
		scn <<= 24;
		scn |= offset;
		return scn;
	}

	/** Setter/Getter methods */
	public String getRelayLogicalSourceName() {
		return relayLogicalSourceName;
	}
	public void setRelayLogicalSourceName(String relayLogicalSourceName) {
		this.relayLogicalSourceName = relayLogicalSourceName;
	}
	public CheckpointPersistenceProvider getCheckpointPersistenceProvider() {
		return checkpointPersistenceProvider;
	}
	public void setCheckpointPersistenceProvider(
			CheckpointPersistenceProvider checkpointPersistenceProvider) {
		this.checkpointPersistenceProvider = checkpointPersistenceProvider;
	}	
	
}
