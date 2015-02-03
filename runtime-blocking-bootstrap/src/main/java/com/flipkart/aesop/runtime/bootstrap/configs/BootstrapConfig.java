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

package com.flipkart.aesop.runtime.bootstrap.configs;

import java.util.Properties;

/**
 * Created by nikhil.bafna on 1/26/15.
 */
public class BootstrapConfig
{
	/** The property name prefix for all Databus bootstrap properties */
	public static final String BOOTSTRAP_PROPERTIES_PREFIX = "databus.bootstrap.";
	private Properties bootstrapProperties = new Properties();

	private int numberOfPartitions;
	private int executorQueueSize;
	private String schemaRegistryLocation;

	public Properties getBootstrapProperties()
	{
		return bootstrapProperties;
	}

	public void setBootstrapProperties(Properties bootstrapProperties)
	{
		this.bootstrapProperties = bootstrapProperties;
	}

	public int getNumberOfPartitions()
	{
		return numberOfPartitions;
	}

	public void setNumberOfPartitions(int numberOfPartitions)
	{
		this.numberOfPartitions = numberOfPartitions;
	}

	public int getExecutorQueueSize()
	{
		return executorQueueSize;
	}

	public void setExecutorQueueSize(int executorQueueSize)
	{
		this.executorQueueSize = executorQueueSize;
	}

	public String getSchemaRegistryLocation()
	{
		return schemaRegistryLocation;
	}

	public void setSchemaRegistryLocation(String schemaRegistryLocation)
	{
		this.schemaRegistryLocation = schemaRegistryLocation;
	}
}
