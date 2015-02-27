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

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DbusPartitionInfo;
import com.linkedin.databus.client.pub.DbusPartitionListener;

/**
 * <code>DefaultPartitionListener</code> is the deafult partition listener. It registers with DatabusRegistration on
 * addition of a new partition
 * @author Jagadeesh Huliyar
 */
public class DefaultPartitionListener implements DbusPartitionListener
{
	private static final Logger logger = LogFactory.getLogger(DefaultPartitionListener.class);

	@Override
	public void onAddPartition(DbusPartitionInfo partitionInfo, DatabusRegistration reg)
	{
		logger.info("New partition is getting added. Partition Id = " + partitionInfo.getPartitionId());
	}

	@Override
	public void onDropPartition(DbusPartitionInfo partitionInfo, DatabusRegistration reg)
	{
		logger.info("Partition is getting dropped. Partition Id = " + partitionInfo.getPartitionId());
	}

}
