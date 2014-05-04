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

import org.springframework.util.Assert;

/**
 * <code>ClientClusterConfig</code> holds Databus configuration properties for a Databus Client Cluster instance. This
 * config treats the
 * properties as opaque and is intended for use as a holder of the information.
 * @author Jagadeesh Huliyar
 */

public class ClientClusterConfig extends ClientConfig
{
	public static final String CLIENT_CLUSTER_NAMESPACE = ClientConfig.CLIENT_PROPERTIES_PREFIX + "clientCluster";

	public static final String CLUSTERNAME = "clusterName";
	public static final String ZKADDR = "zkAddr";
	public static final String NUMPARTITIONS = "numPartitions";
	public static final String QUORUM = "quorum";
	public static final String ZKSESSIONTIMEOUTMS = "zkSessionTimeoutMs";
	public static final String ZKCONNECTIONTIMEOUTMS = "zkConnectionTimeoutMs";
	public static final String CHECKPOINTINTERVALMS = "checkpointIntervalMs";

	private List<ClusterInfoConfig> clusterInfoConfigs;

	/**
	 * @return the clusterInfoConfigs
	 */
	public List<ClusterInfoConfig> getClusterInfoConfigs()
	{
		return clusterInfoConfigs;
	}

	/**
	 * @param clusterInfoConfigs the clusterInfoConfigs to set
	 */
	public void setClusterInfoConfigs(List<ClusterInfoConfig> clusterInfoConfigs)
	{
		this.clusterInfoConfigs = clusterInfoConfigs;

		for (ClusterInfoConfig clusterInfoConfig : this.clusterInfoConfigs)
		{
			this.getClientProperties().put(this.getClusterClientPropertyName(clusterInfoConfig.getId()) + CLUSTERNAME,
			        clusterInfoConfig.getClusterName());
			this.getClientProperties().put(this.getClusterClientPropertyName(clusterInfoConfig.getId()) + ZKADDR,
			        clusterInfoConfig.getZkAddr());
			this.getClientProperties().put(
			        this.getClusterClientPropertyName(clusterInfoConfig.getId()) + NUMPARTITIONS,
			        clusterInfoConfig.getNumPartitions());
			this.getClientProperties().put(this.getClusterClientPropertyName(clusterInfoConfig.getId()) + QUORUM,
			        clusterInfoConfig.getQuorum());
			this.getClientProperties().put(
			        this.getClusterClientPropertyName(clusterInfoConfig.getId()) + ZKSESSIONTIMEOUTMS,
			        clusterInfoConfig.getZkSessionTimeoutMs());
			this.getClientProperties().put(
			        this.getClusterClientPropertyName(clusterInfoConfig.getId()) + ZKCONNECTIONTIMEOUTMS,
			        clusterInfoConfig.getZkConnectionTimeoutMs());
			this.getClientProperties().put(
			        this.getClusterClientPropertyName(clusterInfoConfig.getId()) + CHECKPOINTINTERVALMS,
			        clusterInfoConfig.getCheckpointIntervalMs());
		}
	}

	/**
	 * Interface method implementation. Ensures that all property names start with
	 * {@link ClientConfig#getPropertiesPrefix()}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception
	{
		super.afterPropertiesSet();
		Assert.notNull(this.clusterInfoConfigs,
		        "'clusterInfoConfigs' cannot be null. This Databus Client Cluster will not be initialized");
	}

	/**
	 * Helper method to get the Cluster Client property name appended with the cluster id
	 */
	public String getClusterClientPropertyName(int id)
	{
		return CLIENT_CLUSTER_NAMESPACE + "(" + id + ").";
	}
}
