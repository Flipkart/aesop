package com.flipkart.aesop.bootstrap;

import java.util.List;
import java.util.Map;

import com.flipkart.aesop.eventconsumer.EventConsumerFactoryBean;
import com.flipkart.aesop.bootstrap.configs.BootstrapConfig;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public abstract class AbstractEventProducer implements EventProducer
{
	protected BootstrapConfig bootstrapConfig;
	protected List<String> interestedSourceList;
	protected Map<String, String> tableUriToSrcNameMap;
	protected SchemaRegistryService schemaRegistryService;
	protected EventConsumerFactoryBean eventConsumerFactory;
	protected SourceEventConsumer sourceEventConsumer;

	public BootstrapConfig getBootstrapConfig()
	{
		return bootstrapConfig;
	}

	public void setBootstrapConfig(BootstrapConfig bootstrapConfig)
	{
		this.bootstrapConfig = bootstrapConfig;
	}

	public List<String> getInterestedSourceList()
	{
		return interestedSourceList;
	}

	public void setInterestedSourceList(List<String> interestedSourceList)
	{
		this.interestedSourceList = interestedSourceList;
	}

	public Map<String, String> getTableUriToSrcNameMap()
	{
		return tableUriToSrcNameMap;
	}

	public void setTableUriToSrcNameMap(Map<String, String> tableUriToSrcNameMap)
	{
		this.tableUriToSrcNameMap = tableUriToSrcNameMap;
	}

	public SchemaRegistryService getSchemaRegistryService()
	{
		return schemaRegistryService;
	}

	public void setSchemaRegistryService(SchemaRegistryService schemaRegistryService)
	{
		this.schemaRegistryService = schemaRegistryService;
	}

	public EventConsumerFactoryBean getEventConsumerFactory()
	{
		return eventConsumerFactory;
	}

	public void setEventConsumerFactory(EventConsumerFactoryBean eventConsumerFactory)
	{
		this.eventConsumerFactory = eventConsumerFactory;
	}

	public SourceEventConsumer getSourceEventConsumer()
	{
		return sourceEventConsumer;
	}

	public void setSourceEventConsumer(SourceEventConsumer sourceEventConsumer)
	{
		this.sourceEventConsumer = sourceEventConsumer;
	}
}
