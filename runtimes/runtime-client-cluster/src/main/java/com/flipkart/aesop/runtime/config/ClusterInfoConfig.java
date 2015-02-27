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

package com.flipkart.aesop.runtime.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * <code>ClusterInfoConfig</code> holds Databus configuration properties for a Cluster Client instance. This config treats the
 * properties as opaque and is intended for use as a holder of the information.
 *
 * @author Jagadeesh Huliyar
 */

public class ClusterInfoConfig implements InitializingBean
{
	private Integer id;
	private String clusterName;
	private String zkAddr;
	private Integer numPartitions;
	private Integer keyRangeWidth;
	private Integer quorum;
	private Integer zkSessionTimeoutMs;
	private Integer zkConnectionTimeoutMs;
	private Integer checkpointIntervalMs;

	@Override
    public void afterPropertiesSet() throws Exception
    {
		Assert.notNull(this.id,"'id' cannot be null. The Cluster Client will not be initialized");		
		Assert.notNull(this.clusterName,"'clusterName' cannot be null. The Cluster Client will not be initialized");		
		Assert.notNull(this.zkAddr,"'zkAddr' cannot be null. The Cluster Client will not be initialized");		
		Assert.notNull(this.numPartitions,"'numPartitions' cannot be null. The Cluster Client will not be initialized");		
		Assert.notNull(this.quorum,"'quorum' cannot be null. The Cluster Client will not be initialized");		
		Assert.notNull(this.zkSessionTimeoutMs,"'zkSessionTimeoutMs' cannot be null. The Cluster Client will not be initialized");		
		Assert.notNull(this.zkConnectionTimeoutMs,"'zkConnectionTimeoutMs' cannot be null. The Cluster Client will not be initialized");		
		Assert.notNull(this.checkpointIntervalMs,"'checkpointIntervalMs' cannot be null. The Cluster Client will not be initialized");		
    }

	/**
	 * @return the id
	 */
	public Integer getId()
	{
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(Integer id)
	{
		this.id = id;
	}

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
	 * @return the zkAddr
	 */
	public String getZkAddr()
	{
		return zkAddr;
	}

	/**
	 * @param zkAddr the zkAddr to set
	 */
	public void setZkAddr(String zkAddr)
	{
		this.zkAddr = zkAddr;
	}

	/**
	 * @return the numPartitions
	 */
	public Integer getNumPartitions()
	{
		return numPartitions;
	}

	/**
	 * @param numPartitions the numPartitions to set
	 */
	public void setNumPartitions(Integer numPartitions)
	{
		this.numPartitions = numPartitions;
	}

	/**
	 * @return the quorum
	 */
	public Integer getQuorum()
	{
		return quorum;
	}

	/**
	 * @param quorum the quorum to set
	 */
	public void setQuorum(Integer quorum)
	{
		this.quorum = quorum;
	}

	/**
	 * @return the zkSessionTimeoutMs
	 */
	public Integer getZkSessionTimeoutMs()
	{
		return zkSessionTimeoutMs;
	}

	/**
	 * @param zkSessionTimeoutMs the zkSessionTimeoutMs to set
	 */
	public void setZkSessionTimeoutMs(Integer zkSessionTimeoutMs)
	{
		this.zkSessionTimeoutMs = zkSessionTimeoutMs;
	}

	/**
	 * @return the zkConnectionTimeoutMs
	 */
	public Integer getZkConnectionTimeoutMs()
	{
		return zkConnectionTimeoutMs;
	}

	/**
	 * @param zkConnectionTimeoutMs the zkConnectionTimeoutMs to set
	 */
	public void setZkConnectionTimeoutMs(Integer zkConnectionTimeoutMs)
	{
		this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
	}

	/**
	 * @return the checkpointIntervalMs
	 */
	public Integer getCheckpointIntervalMs()
	{
		return checkpointIntervalMs;
	}

	/**
	 * @param checkpointIntervalMs the checkpointIntervalMs to set
	 */
	public void setCheckpointIntervalMs(Integer checkpointIntervalMs)
	{
		this.checkpointIntervalMs = checkpointIntervalMs;
	}

	public Integer getKeyRangeWidth()
	{
		return keyRangeWidth;
	}

	public void setKeyRangeWidth(Integer keyRangeWidth)
	{
		this.keyRangeWidth = keyRangeWidth;
	}
}
