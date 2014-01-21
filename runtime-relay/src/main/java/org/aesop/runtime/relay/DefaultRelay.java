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
import java.util.ArrayList;
import java.util.List;

import org.aesop.runtime.config.ProducerRegistration;
import org.aesop.runtime.producer.AbstractEventProducer;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.producers.EventProducer;
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
	
    /** The SCN reader-writer*/
    protected MultiServerSequenceNumberHandler maxScnReaderWriters;
    
	/** The ProducerRegistration list for the Relay*/
    protected List<ProducerRegistration> producerRegistrationList = new ArrayList<ProducerRegistration>();    
    
	/**
	 * Constructor for this class. Invokes constructor of the super-type with the passed-in arguments
	 */
    public DefaultRelay(StaticConfig config, PhysicalSourceStaticConfig[] pConfigs, SourceIdNameRegistry sourcesIdNameRegistry,
            SchemaRegistryService schemaRegistry) throws IOException, InvalidConfigException, DatabusException {
    	super(config, pConfigs, sourcesIdNameRegistry, schemaRegistry);
    }
    
    /**
     * Overriden superclass method. Starts up the registered Producers after calling super.doStart()
     * @see com.linkedin.databus.container.netty.HttpRelay#doStart()
     */
    protected void doStart() {
    	super.doStart();
    	for (ProducerRegistration producerRegistration : this.producerRegistrationList) {
    		EventProducer producer = producerRegistration.getEventProducer();
    		long startScn = -1;
    		if (AbstractEventProducer.class.isAssignableFrom(producer.getClass())) {
    			try {
    				startScn = ((AbstractEventProducer)producer).getMaxScnReaderWriter().getMaxScn();
    			} catch(Exception e) {
    				LOGGER.error("Error starting producer : '" + ((AbstractEventProducer)producer).getName() + "'. Producer not started.", e);
    				continue;
    			}
    		}
    		producer.start(startScn);
    	}
    }

	/** Getter/Setter methods to override default implementations of various components used by this Relay*/
	public MultiServerSequenceNumberHandler getMaxScnReaderWriters() {
		return this.maxScnReaderWriters;
	}
	public void setMaxScnReaderWriters(
			MultiServerSequenceNumberHandler maxScnReaderWriters) {
		this.maxScnReaderWriters = maxScnReaderWriters;
	}
	public void setProducerRegistrationList(List<ProducerRegistration> producerRegistrationList) {
		this.producerRegistrationList = producerRegistrationList;
	}
	public List<ProducerRegistration> getProducerRegistrationList() {
		return this.producerRegistrationList;
	}
	
}
