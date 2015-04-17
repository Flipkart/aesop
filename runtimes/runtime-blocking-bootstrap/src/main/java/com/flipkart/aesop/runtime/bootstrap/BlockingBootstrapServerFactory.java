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

import com.flipkart.aesop.runtime.bootstrap.configs.BootstrapConfig;
import com.flipkart.aesop.runtime.bootstrap.consumer.SourceEventConsumer;
import com.flipkart.aesop.runtime.bootstrap.producer.BlockingEventProducer;
import com.flipkart.aesop.runtime.bootstrap.producer.registeration.ProducerRegistration;
import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import org.springframework.beans.factory.FactoryBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * The Spring factory bean for creating {@link BlockingBootstrapServer} instances based on configured properties
 * @author nrbafna
 */
public class BlockingBootstrapServerFactory implements FactoryBean<BlockingBootstrapServer>
{
    private BootstrapConfig bootstrapConfig;

    private SourceEventConsumer consumer;
    /** The ProducerRegistration list for the Relay*/
    private List<ProducerRegistration> producerRegistrationList = new ArrayList<ProducerRegistration>();

    @Override
    public BlockingBootstrapServer getObject() throws Exception
    {
        /* HTTP RELAY */
        HttpRelay.Config httpConfig = new HttpRelay.Config();
        ConfigLoader<HttpRelay.StaticConfig> staticConfigLoader = new ConfigLoader<HttpRelay.StaticConfig>(BootstrapConfig.BOOTSTRAP_PROPERTIES_PREFIX, httpConfig);

        //Making this a list to initialise each producer seperately with initial SCN
        HttpRelay.StaticConfig[] staticConfigList = new HttpRelay.StaticConfig[this.producerRegistrationList.size()];

        PhysicalSourceStaticConfig[] pStaticConfigs = new PhysicalSourceStaticConfig[this.producerRegistrationList.size()];

        /* SCHEMA REGISTRY */
        FileSystemSchemaRegistryService.Config configBuilder = new FileSystemSchemaRegistryService.Config();
        configBuilder.setFallbackToResources(true);
        configBuilder.setSchemaDir(this.bootstrapConfig.getSchemaRegistryLocation());
        FileSystemSchemaRegistryService.StaticConfig schemaRegistryServiceConfig = configBuilder.build();
        FileSystemSchemaRegistryService schemaRegistryService = FileSystemSchemaRegistryService.build(schemaRegistryServiceConfig);

        /* MAX SCN READER WRITER */
            for (int i=0; i < this.producerRegistrationList.size(); i++) {
                //Get Properties from Relay Config
                Properties mergedProperties = new Properties();
                mergedProperties.putAll(this.bootstrapConfig.getBootstrapProperties());

                // Obtain Properties from Product Registration if it exists
                if(producerRegistrationList.get(i).getProperties() != null) {
                    mergedProperties.putAll(producerRegistrationList.get(i).getProperties());
                }
                staticConfigList[i] = staticConfigLoader.loadConfig(mergedProperties);
                pStaticConfigs[i] = this.producerRegistrationList.get(i).getPhysicalSourceConfig().build();
            }

        /* Setting relevant details into the Blocking Event Producer */
        BlockingBootstrapServer bootstrapServer = new BlockingBootstrapServer(staticConfigList[0],pStaticConfigs,
                SourceIdNameRegistry.createFromIdNamePairs(staticConfigList[0].getSourceIds()),schemaRegistryService);

        int i=0;
        for (ProducerRegistration producerRegistration :  this.producerRegistrationList) {

            EventProducer producer = producerRegistration.getEventProducer();
            if (BlockingEventProducer.class.isAssignableFrom(producer.getClass())) {

                BlockingEventProducer blockingEventProducer =  ((BlockingEventProducer) producer);
                blockingEventProducer.registerConsumer(consumer);
                blockingEventProducer.setPhysicalSourceConfig(producerRegistration.getPhysicalSourceConfig());
                blockingEventProducer.setSchemaRegistryService(schemaRegistryService);

                /* Setting Http Config Source Name */
                for (LogicalSourceConfig logicalSourceConfig :producerRegistration.getPhysicalSourceConfig().getSources()) {
                    httpConfig.setSourceName(String.valueOf(logicalSourceConfig.getId()), logicalSourceConfig.getName());
                }
                blockingEventProducer.setDbusEventsStatisticsCollector(bootstrapServer.getInboundEventStatisticsCollector());
                blockingEventProducer.registerMetricsCollector(bootstrapServer.getMetricsCollector());
            }
        }

        bootstrapServer.registerConsumer(consumer);
        bootstrapServer.setProducerRegistrationList(producerRegistrationList);
        return bootstrapServer;
    }

    @Override
    public Class<?> getObjectType()
    {
        return BlockingBootstrapServer.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }

    public void setBootstrapConfig(BootstrapConfig bootstrapConfig) {
        this.bootstrapConfig = bootstrapConfig;
    }

    public void setConsumer(SourceEventConsumer consumer){
        this.consumer = consumer;
    }

    public void setProducerRegistrationList(List<ProducerRegistration> producerRegistrationList) {
        this.producerRegistrationList = producerRegistrationList;
    }
    public List<ProducerRegistration> getProducerRegistrationList() {
        return this.producerRegistrationList;
    }
}
