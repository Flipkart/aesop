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
package com.flipkart.aesop.runtime.client;

import java.io.IOException;
import java.util.List;

import org.trpr.platform.core.PlatformException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.core.BootstrapCheckpointHandler;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;

/**
 * The <code>DefaultClient</code> class defines behavior of a default Databus Relay Client. 
 * 
 * @author Regunath B
 * @version 1.0, 23 Jan 2014
 */

public class DefaultClient extends DatabusHttpClientImpl {

	/** Logger for this class*/
	protected static final Logger LOGGER = LogFactory.getLogger(DefaultClient.class);
	
	/**
	 * Constructor for this class. Invokes constructor of the super-type with the passed-in arguments
	 */	
	public DefaultClient(StaticConfig config) throws InvalidConfigException,IOException, DatabusException {
		super(config);
	}
	
	/**
	 * Overriden superclass method. Creates the Bootstrap checkpoint using the last seen SCN (or) 0, if none is found, if bootstrapping is enabled.
	 * This client will then go into Relay "fall off" mode and start with Bootstrap, move on to Catchup and then finally onto Online consumption.
	 * @see com.linkedin.databus.client.DatabusHttpClientImpl#doStart()
	 */
	protected void doStart() {
		for(List<DatabusSubscription> subscriptionList: this._relayGroups.keySet()) {
			BootstrapCheckpointHandler bstCheckpointHandler = new BootstrapCheckpointHandler(DatabusSubscription.getStrList(subscriptionList).toArray(new String[0]));
			if (!this._bootstrapGroups.isEmpty()) {
				Checkpoint bootstrapCheckpoint = new Checkpoint();
				// check if a persistent checkpoint exists for this source
				Checkpoint persistentCheckpoint = this.getCheckpointPersistenceProvider().loadCheckpoint(DatabusSubscription.getStrList(subscriptionList));
				bootstrapCheckpoint = bstCheckpointHandler.createInitialBootstrapCheckpoint(bootstrapCheckpoint, 
						persistentCheckpoint != null ? persistentCheckpoint.getWindowScn() : 0L);
				bootstrapCheckpoint.setWindowScn(bootstrapCheckpoint.getBootstrapSinceScn());
				// create this checkpoint in the persistence location
				this.getCheckpointPersistenceProvider().removeCheckpoint(DatabusSubscription.getStrList(subscriptionList));
				try {
					this.getCheckpointPersistenceProvider().storeCheckpoint(DatabusSubscription.getStrList(subscriptionList), bootstrapCheckpoint);
				} catch (IOException e) {
					LOGGER.error("Error handling checkpoint information for client : " + this.getClientStaticConfig(), e);
					throw new PlatformException("Error starting up Databus client : " + this.getClientStaticConfig(), e);
				}
			}
		}
		super.doStart();
	}

}
