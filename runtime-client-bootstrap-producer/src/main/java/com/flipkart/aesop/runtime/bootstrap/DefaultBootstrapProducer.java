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
package com.flipkart.aesop.runtime.bootstrap;

import java.io.IOException;
import java.sql.SQLException;

import com.linkedin.databus.bootstrap.producer.BootstrapProducerStaticConfig;
import com.linkedin.databus.bootstrap.producer.DatabusBootstrapProducer;
import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.BootstrapDBException;

/**
 * The <code>DefaultBootstrapProducer</code> class defines behavior of a default Databus Relay Client that acts as a producer for Bootstrap data i.e. consumes
 * change events and creates snapshots that may be used to bootstrap slow databus consumers {@link DatabusBootstrapConsumer}
 * 
 * @author Regunath B
 * @version 1.0, 06 Feb 2014
 */

public class DefaultBootstrapProducer extends DatabusBootstrapProducer {

	/**
	 * Constructor for this class. Invokes constructor of the super-type with the passed-in arguments
	 */
	public DefaultBootstrapProducer(BootstrapProducerStaticConfig bootstrapProducerStaticConfig)
			throws IOException, InvalidConfigException, InstantiationException, IllegalAccessException,
		    ClassNotFoundException, SQLException, DatabusClientException, DatabusException,
		    BootstrapDBException {
		super(bootstrapProducerStaticConfig);
	}

}
