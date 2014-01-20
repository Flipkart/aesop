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
package org.aesop.runtime.relay;

import java.io.IOException;
import java.util.Map;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.RelayEventProducersRegistry;
import com.linkedin.databus2.relay.DatabusRelayMain;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;

/**
 * The <code>DefaultRelay</code> class defines behavior of a default Databus Relay. Provides methods to register
 * one or more Databus Change Event producers. Also propagates all lifecycle commands on this Relay to all registered
 * event producers. This implementation is based on the Databus {@link DatabusRelayMain} source but differs in the following
 * aspects:
 * <pre><ul>
 * <li>Does not interpret producer implementations from URI pattern such as Oracle and GoldenGate</li>
 * <li>Does not start or otherwise interpret flags for the initializing the DB Puller</li>
 * </ul><pre>
 * 
 * @author Regunath B
 * @version 1.0, 08 Jan 2014
 */
public class DefaultRelay extends HttpRelay {

	/** Logger for this class*/
	protected static final Logger LOGGER = LogFactory.getLogger(DefaultRelay.class);
	
	/** The event producers registry*/
    protected RelayEventProducersRegistry producersRegistry;	
	
    /** The SCN reader-writer*/
    protected MultiServerSequenceNumberHandler maxScnReaderWriters;
    
    /** Map of Event Producers keyed by physical partitions*/
	protected Map<PhysicalPartition, EventProducer> producersMap;        

	/**
	 * Constructor for this class. Invokes constructor of the super-type with the passed-in arguments
	 */
    public DefaultRelay(StaticConfig config, PhysicalSourceStaticConfig[] pConfigs, SourceIdNameRegistry sourcesIdNameRegistry,
            SchemaRegistryService schemaRegistry) throws IOException, InvalidConfigException, DatabusException {
    	super(config, pConfigs, sourcesIdNameRegistry, schemaRegistry);
    }

	/** Getter/Setter methods to override default implementations of various components used by this Relay*/
	public RelayEventProducersRegistry getProducersRegistry() {
		return this.producersRegistry;
	}
	public void setProducersRegistry(RelayEventProducersRegistry producersRegistry) {
		this.producersRegistry = producersRegistry;
	}
	public MultiServerSequenceNumberHandler getMaxScnReaderWriters() {
		return this.maxScnReaderWriters;
	}
	public void setMaxScnReaderWriters(
			MultiServerSequenceNumberHandler maxScnReaderWriters) {
		this.maxScnReaderWriters = maxScnReaderWriters;
	}
	public Map<PhysicalPartition, EventProducer> getProducersMap() {
		return this.producersMap;
	}
}
