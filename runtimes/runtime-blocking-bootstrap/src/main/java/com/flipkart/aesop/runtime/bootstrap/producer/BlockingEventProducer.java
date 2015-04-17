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

package com.flipkart.aesop.runtime.bootstrap.producer;

import com.flipkart.aesop.runtime.bootstrap.configs.BootstrapConfig;
import com.flipkart.aesop.runtime.bootstrap.consumer.SourceEventConsumer;
import com.flipkart.aesop.runtime.bootstrap.metrics.MetricsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <code>BlockingEventProducer</code> produces list of {@link com.flipkart.aesop.event.AbstractEvent}, filters them
 * using registered interested sources & submits to the registered event consumer.
 * @author nrbafna
 */
public abstract class BlockingEventProducer implements EventProducer
{
    /** Name of this event producer*/
    protected String name;
    /* Bootstrap Producer Configs */
    protected BootstrapConfig bootstrapConfig;

    /** Source related member variables*/
    protected PhysicalSourceConfig physicalSourceConfig;
    protected PhysicalSourceStaticConfig physicalSourceStaticConfig;
    protected SchemaRegistryService schemaRegistryService;

    /* Source Event Consumer */
    protected SourceEventConsumer sourceEventConsumer;

    /* DB Event Stats Collector */
    protected DbusEventsStatisticsCollector dbusEventsStatisticsCollector;

    /* Metrics Collector */
    protected MetricsCollector metricsCollector;

    /** Event-handling related member variables*/
    protected AtomicLong sinceSCN = new AtomicLong(-1); // This is the previous SCN to which it was read.

    public BootstrapConfig getBootstrapConfig() {
        return bootstrapConfig;
    }

    public void setBootstrapConfig(BootstrapConfig bootstrapConfig) {
        this.bootstrapConfig = bootstrapConfig;
    }

    /** Getter/Setter methods */
    public String getName() {
        return this.name;
    }
    public long getSCN() {
        return this.sinceSCN.get();
    }
    public void setSchemaRegistryService(SchemaRegistryService schemaRegistryService) throws Exception {
        this.schemaRegistryService = schemaRegistryService;
        this.physicalSourceStaticConfig = this.physicalSourceConfig.build();
    }
    public void setPhysicalSourceConfig(PhysicalSourceConfig physicalSourceConfig) {
        this.physicalSourceConfig = physicalSourceConfig;
        this.name = this.physicalSourceConfig.getName();
    }
    public PhysicalSourceStaticConfig getPhysicalSourceStaticConfig() {
        return physicalSourceStaticConfig;
    }
    public void registerConsumer(SourceEventConsumer consumer) {
        this.sourceEventConsumer = consumer;
    }
    public void setDbusEventsStatisticsCollector(DbusEventsStatisticsCollector dbusEventsStatisticsCollector) {
		this.dbusEventsStatisticsCollector = dbusEventsStatisticsCollector;
	}
     public void registerMetricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }
}
