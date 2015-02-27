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
package com.flipkart.aesop.clusterclient.sample.consumer;

import com.flipkart.aesop.sample.client.common.consumer.ConsoleAppenderEventConsumer;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DbusClusterConsumerFactory;
import com.linkedin.databus.client.pub.DbusClusterInfo;
import com.linkedin.databus.client.pub.DbusPartitionInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * <code>ConsumerFactory</code> is a sub-type of {@link DbusClusterConsumerFactory} crates consumers
 * Person change event type.
 * @author Jagadeesh Huliyar
 */
public class ConsumerFactory implements DbusClusterConsumerFactory
{
	@Override
	public Collection<DatabusCombinedConsumer> createPartitionedConsumers(DbusClusterInfo clusterInfo,
	        DbusPartitionInfo partitionInfo)
	{
		DatabusCombinedConsumer personConsumer = new ConsoleAppenderEventConsumer();
		List<DatabusCombinedConsumer> list = new ArrayList<DatabusCombinedConsumer>();
		list.add(personConsumer);
		return list;
	}

}
