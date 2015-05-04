/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.aesop.runtime.bootstrap;

import com.flipkart.aesop.runtime.bootstrap.consumer.SourceEventConsumer;
import com.flipkart.aesop.runtime.bootstrap.metrics.MetricsCollector;
import com.flipkart.aesop.runtime.bootstrap.producer.registeration.ProducerRegistration;
import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdmin;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The <code>BlockingBootstrapServer</code> class defines behavior of a blocking bootstrap server i.e. serves
 * change data snapshots that may be used to bootstrap slow consumers
 * @author nrbafna
 */
public class BlockingBootstrapServer extends HttpRelay
{
    public static final Logger LOGGER = LogFactory.getLogger(BlockingBootstrapServer.class);

    /** The ProducerRegistration list for the Relay*/
    protected List<ProducerRegistration> producerRegistrationList = new ArrayList<ProducerRegistration>();

    /*Source Event Consumer */
    private SourceEventConsumer consumer;

     /** metrics collector */
    private MetricsCollector metricsCollector;

    /**
	 * Constructor for this class. Invokes constructor of the super-type with the passed-in arguments
	 */
    public BlockingBootstrapServer(StaticConfig config, PhysicalSourceStaticConfig[] pConfigs, SourceIdNameRegistry sourcesIdNameRegistry,
            SchemaRegistryService schemaRegistry) throws IOException, DatabusException {
    	super(config, pConfigs, sourcesIdNameRegistry, schemaRegistry);
        metricsCollector = new MetricsCollector(this);
    }

    @Override
    protected DatabusComponentAdmin createComponentAdmin()
    {
        return new DatabusComponentAdmin(this, getMbeanServer(), HttpRelay.class.getSimpleName());
    }

    @Override
    public void pause()
    {
        getComponentStatus().pause();
        for (ProducerRegistration producerRegistration : this.producerRegistrationList){
            producerRegistration.getEventProducer().pause();
        }
    }

    @Override
    public void resume()
    {
        getComponentStatus().resume();
        for (ProducerRegistration producerRegistration : this.producerRegistrationList){
            producerRegistration.getEventProducer().unpause();
        }
    }

    @Override
    public void suspendOnError(Throwable throwable)
    {
        getComponentStatus().suspendOnError(throwable);
    }

    @Override
    protected void doStart()
    {
        super.doStart();

        for (ProducerRegistration producerRegistration : this.producerRegistrationList) {
            EventProducer producer = producerRegistration.getEventProducer();
            producer.start(Long.valueOf(String.valueOf(producerRegistration.getProperties().
                    get("databus.bootstrap.dataSources.sequenceNumbersHandler.file.initVal"))));
        }
        this.registerShutdownHook();
    }

    @Override
    protected void doShutdown()
    {
        for (ProducerRegistration producerRegistration : this.producerRegistrationList){
            producerRegistration.getEventProducer().shutdown();
        }
        consumer.shutdown();
        super.doShutdown();
    }

    public MetricsCollector getMetricsCollector() { return metricsCollector; }
    public List<ProducerRegistration> getProducerRegistrationList() {
		return this.producerRegistrationList;
	}
    public void setProducerRegistrationList(List<ProducerRegistration> producerRegistrationList) {
        this.producerRegistrationList = producerRegistrationList;
    }
    public void registerConsumer(SourceEventConsumer consumer) {
        this.consumer = consumer;
    }
}
