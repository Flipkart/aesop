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

import com.linkedin.databus.bootstrap.server.BootstrapHttpServer;
import com.linkedin.databus.bootstrap.server.BootstrapServerStaticConfig;
import com.linkedin.databus2.core.DatabusException;

/**
 * The <code>DefaultBootstrapServer</code> class defines behavior of a default Databus Bootstrap server i.e. serves change data snapshots that may be used
 * to bootstrap slow databus consumers {@link DatabusBootstrapConsumer}
 * 
 * @author Regunath B
 * @version 1.0, 12 Feb 2014
 */
public class DefaultBootstrapServer extends BootstrapHttpServer {
	
	/**
	 * Constructor for this class. Invokes constructor of the super-type with the passed-in arguments
	 */	
	public DefaultBootstrapServer(BootstrapServerStaticConfig bootstrapServerConfig) throws DatabusException, IOException {
		super(bootstrapServerConfig);
	}
	
}
