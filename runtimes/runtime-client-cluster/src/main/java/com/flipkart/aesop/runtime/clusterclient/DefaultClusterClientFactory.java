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

package com.flipkart.aesop.runtime.clusterclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.client.DefaultClient;
import com.flipkart.aesop.runtime.config.ClientClusterConfig;
import com.flipkart.aesop.runtime.config.ClusterRegistration;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DbusClusterConsumerFactory;
import com.linkedin.databus.client.pub.DbusModPartitionedFilterFactory;
import com.linkedin.databus.client.pub.DbusPartitionListener;
import com.linkedin.databus.client.pub.DbusServerSideFilterFactory;
import com.linkedin.databus.core.util.ConfigLoader;

/**
 * The Spring factory bean for creating {@link DefaultClient} instances based on configured properties
 * @author Jagadeesh Huliyar
 */

public class DefaultClusterClientFactory implements FactoryBean<DefaultClient>, InitializingBean
{
	private ClientClusterConfig clientClusterConfig;
	private List<ClusterRegistration> clusterRegistrations;
	private List<DatabusRegistration> databusRegistrations = new ArrayList<DatabusRegistration>();
	private DefaultClient databusClient;
	private static final Logger logger = LogFactory.getLogger(DefaultClusterClientFactory.class);

	/**
	 * Interface method implementation. Checks for mandatory dependencies and initializes this Relay Client
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception
	{
		Assert.notNull(this.clientClusterConfig,
		        "'clientClusterConfig' cannot be null. This Relay Cluster Client will not be initialized");
		Assert.notEmpty(this.clusterRegistrations, "'clusterRegistrations' cannot be empty. No Cluster Registrations.");
	}

	@Override
	public DefaultClient getObject() throws Exception
	{
		DatabusHttpClientImpl.Config config = new DatabusHttpClientImpl.Config();
		ConfigLoader<DatabusHttpClientImpl.StaticConfig> staticConfigLoader =
		        new ConfigLoader<DatabusHttpClientImpl.StaticConfig>(clientClusterConfig.getClientPropertiesPrefix(),
		                config);
		Properties properties = this.clientClusterConfig.getClientProperties();
		DatabusHttpClientImpl.StaticConfig staticConfig = staticConfigLoader.loadConfig(properties);
		databusClient = new DefaultClient(staticConfig);
		// register all Cluster registrations with the Relay Client
		for (ClusterRegistration clusterRegistration : clusterRegistrations)
		{
			String clusterName = clusterRegistration.getClusterName();
			DbusPartitionListener partitionListener = clusterRegistration.getPartitionListener();
			DbusClusterConsumerFactory consumerFactory = clusterRegistration.getConsumerFactory();
			List<String> logicalSources = clusterRegistration.getLogicalSources();
			String[] logicalSourcesArr = logicalSources.toArray(new String[0]);
			DbusServerSideFilterFactory filterFactory = clusterRegistration.getFilterFactory();
			if (filterFactory == null)
			{
				filterFactory = new DbusModPartitionedFilterFactory(logicalSourcesArr);
			}
			DatabusRegistration registration = databusClient.registerCluster(clusterName, consumerFactory, filterFactory, partitionListener, logicalSourcesArr);
			databusRegistrations.add(registration);
		}
		//Start all registrations now
		for(DatabusRegistration databusRegistration : databusRegistrations)
		{
			databusRegistration.start();
		}
		logger.info("Adding shutdown hook");
		Runtime.getRuntime().addShutdownHook(new ClusterShutDownHookThread());
		logger.info("Added shutdown hook");		
		return databusClient;
	}
	
	private class ClusterShutDownHookThread extends Thread
	{
		@Override
	    public void run()
	    {
			logger.info("ClusterShutDownHookThread getting executed. Shutting down Client");
			databusClient.shutdown();
			logger.info("ClusterShutDownHookThread executed. Client shutdown complete");
	    }
	}

	@Override
	public Class<?> getObjectType()
	{
		return DefaultClient.class;
	}

	@Override
	public boolean isSingleton()
	{
		return true;
	}

	/**
	 * @return the clientClusterConfig
	 */
	public ClientClusterConfig getClientClusterConfig()
	{
		return clientClusterConfig;
	}

	/**
	 * @param clientClusterConfig the clientClusterConfig to set
	 */
	public void setClientClusterConfig(ClientClusterConfig clientClusterConfig)
	{
		this.clientClusterConfig = clientClusterConfig;
	}

	/**
	 * @return the clusterRegistrations
	 */
	public List<ClusterRegistration> getClusterRegistrations()
	{
		return clusterRegistrations;
	}

	/**
	 * @param clusterRegistrations the clusterRegistrations to set
	 */
	public void setClusterRegistrations(List<ClusterRegistration> clusterRegistrations)
	{
		this.clusterRegistrations = clusterRegistrations;
	}

}
