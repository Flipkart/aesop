package com.flipkart.aesop.bootstrap.configs;

/**
 * Created by nikhil.bafna on 1/26/15.
 */
public class BootstrapConfig
{
	private int numberOfPartitions;
	private int executorQueueSize;
	private String schemaRegistryLocation;

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
