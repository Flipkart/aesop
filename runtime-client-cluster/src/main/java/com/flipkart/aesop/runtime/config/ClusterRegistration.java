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

package com.flipkart.aesop.runtime.config;

import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.aesop.runtime.clusterclient.DefaultPartitionListener;
import com.linkedin.databus.client.pub.DbusClusterConsumerFactory;
import com.linkedin.databus.client.pub.DbusPartitionListener;
import com.linkedin.databus.client.pub.DbusServerSideFilterFactory;

/**
 * <code>ClusterRegistration</code> holds information for registering to a Databus Client Cluster
 * @author Jagadeesh Huliyar
 */
public class ClusterRegistration implements InitializingBean
{
	private String clusterName;
	private DbusClusterConsumerFactory consumerFactory;
	DbusServerSideFilterFactory filterFactory;
	private DbusPartitionListener partitionListener = new DefaultPartitionListener();
	private List<String> logicalSources;

	/**
	 * @return the clusterName
	 */
	public String getClusterName()
	{
		return clusterName;
	}

	/**
	 * @param clusterName the clusterName to set
	 */
	public void setClusterName(String clusterName)
	{
		this.clusterName = clusterName;
	}

	/**
	 * @return the consumerFactory
	 */
	public DbusClusterConsumerFactory getConsumerFactory()
	{
		return consumerFactory;
	}

	/**
	 * @param consumerFactory the consumerFactory to set
	 */
	public void setConsumerFactory(DbusClusterConsumerFactory consumerFactory)
	{
		this.consumerFactory = consumerFactory;
	}

	public DbusServerSideFilterFactory getFilterFactory()
	{
		return filterFactory;
	}

	public void setFilterFactory(DbusServerSideFilterFactory filterFactory)
	{
		this.filterFactory = filterFactory;
	}

	/**
	 * @return the partitionListener
	 */
	public DbusPartitionListener getPartitionListener()
	{
		return partitionListener;
	}

	/**
	 * @param partitionListener the partitionListener to set
	 */
	public void setPartitionListener(DbusPartitionListener partitionListener)
	{
		this.partitionListener = partitionListener;
	}

	/**
	 * @return the sources
	 */
	public List<String> getLogicalSources()
	{
		return logicalSources;
	}

	/**
	 * @param sources the sources to set
	 */
	public void setLogicalSources(List<String> logicalSources)
	{
		this.logicalSources = logicalSources;
	}

	@Override
	public void afterPropertiesSet() throws Exception
	{
		Assert.notNull(this.clusterName,"'eventConsumer' cannot be null. An AbstractDatabusCombinedConsumer must be specified");
		Assert.notNull(this.consumerFactory, "'consumerFactory' cannot be null. Logical sources must be specified");
		Assert.notNull(this.logicalSources, "'logicalSources' cannot be null. Logical sources must be specified");
	}
}
