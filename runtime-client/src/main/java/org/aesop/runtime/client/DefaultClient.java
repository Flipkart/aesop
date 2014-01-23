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
package org.aesop.runtime.client;

import java.io.IOException;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;

/**
 * The <code>DefaultClient</code> class defines behavior of a default Databus Relay Client. Provides methods to register
 * one or more Databus Change Event consumers. Also propagates all lifecycle commands on this Relay Client to all registered
 * event consumers. 
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

}
