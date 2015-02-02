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
