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
package com.flipkart.aesop.runtime.relay;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.config.ProducerRegistration;
import com.flipkart.aesop.runtime.metrics.MetricsCollector;
import com.flipkart.aesop.runtime.producer.AbstractEventProducer;
import com.flipkart.aesop.runtime.relay.netty.HttpRelayPipelineFactory;
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

    /** metrics collector */
    private MetricsCollector metricsCollector;
    
    /** List of disconnected peers. We'll use a copy on write list to deal with concurrency. writes are low on this list*/
    private List<String> disconnectedPeers = new CopyOnWriteArrayList<String>();
    
	/**
	 * Constructor for this class. Invokes constructor of the super-type with the passed-in arguments
	 */
    public DefaultRelay(StaticConfig config, PhysicalSourceStaticConfig[] pConfigs, SourceIdNameRegistry sourcesIdNameRegistry,
            SchemaRegistryService schemaRegistry) throws IOException, InvalidConfigException, DatabusException {
    	super(config, pConfigs, sourcesIdNameRegistry, schemaRegistry);
        metricsCollector = new MetricsCollector(this);
    }
    
    /**
     * Overriden superclass method. Calls pause on the registered Producers after calling super.pause()
     * @see com.linkedin.databus.container.netty.HttpRelay#pause()
     */
    public void pause() {
    	super.pause();
    	for (ProducerRegistration producerRegistration : this.producerRegistrationList) {
    		producerRegistration.getEventProducer().pause();
    	}    	
    }
    /**
     * Overriden superclass method. Calls resume on the registered Producers after calling super.resume()
     * @see com.linkedin.databus.container.netty.HttpRelay#resume()
     */
    public void resume() {
    	super.resume();
    	for (ProducerRegistration producerRegistration : this.producerRegistrationList) {
    		producerRegistration.getEventProducer().unpause();
    	}    	
    }
    /**
     * Overriden superclass method. Calls shutdown on the registered Producers after calling super.suspendOnError()
     * @see com.linkedin.databus.container.netty.HttpRelay#suspendOnError(java.lang.Throwable)
     */
    public void suspendOnError(Throwable cause) {
    	super.suspendOnError(cause);
    	for (ProducerRegistration producerRegistration : this.producerRegistrationList) {
    		producerRegistration.getEventProducer().shutdown();
    	}    	
    }  
    
    /**
     * Gets a list of all known connected peers
     * @return List of peer names
     */
    public List<String> getPeers() {
    	List<String> allPeers = getHttpStatisticsCollector().getPeers();
    	for (String disconnectedPeer : this.disconnectedPeers) {
    		allPeers.remove(disconnectedPeer);
    	}
    	return allPeers;
    }

    /**
     * Informs this Relay of a peer connection
     * @param peer the connected peer
     */
    public void firePeerConnect(String peer) {
    	this.disconnectedPeers.remove(peer);
    	LOGGER.debug("Disconnected peers in 'firePeerConnect'" + this.disconnectedPeers);
    }
    
    /**
     * Informs this Relay of a disconnected peer
     * @param peer the disconnected peer
     */
    public void firePeerDisconnect(String peer) {
    	this.disconnectedPeers.add(peer);
    	LOGGER.debug("Disconnected peers in 'firePeerDisconnect'" + this.disconnectedPeers);
    }

    /**
     * Overriden superclass method. Creates and uses the Aesop {@link HttpRelayPipelineFactory} instead of the Databus 
     * {@link com.linkedin.databus.container.netty.HttpRelayPipelineFactory}
     * @see com.linkedin.databus.container.netty.HttpRelay#initializeRelayNetworking()
     */
    protected void initializeRelayNetworking() throws IOException, DatabusException {
      _httpBootstrap.setPipelineFactory(new HttpRelayPipelineFactory(this, _httpBootstrap.getPipelineFactory()));
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
    	this.registerShutdownHook();
    }
    
    /**
     * Overriden superclass method. Stops the registered Producers after calling super.doShutdown()
     * @see com.linkedin.databus.container.netty.HttpRelay#doShutdown()
     */
	protected void doShutdown(){
		LOGGER.info("Shutting down Relay");
		for (ProducerRegistration producerRegistration : this.producerRegistrationList){
			producerRegistration.getEventProducer().shutdown();
		}
		LOGGER.info("All producers shutdown completed");
		super.doShutdown();
		LOGGER.info("Relay shutdown completed");
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
    public MetricsCollector getMetricsCollector() { return metricsCollector; }
	
}
