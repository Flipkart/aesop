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

package com.flipkart.aesop.runtime.bootstrap.producer;

import java.util.List;
import java.util.Map;

import com.flipkart.aesop.runtime.bootstrap.configs.BootstrapConfig;
import com.flipkart.aesop.runtime.bootstrap.consumer.SourceEventConsumer;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * <code>BlockingEventProducer</code> produces list of {@link com.flipkart.aesop.event.AbstractEvent}, filters them
 * using registered interested sources & submits to the registered event consumer.
 * @author nrbafna
 */
public abstract class BlockingEventProducer implements EventProducer
{
	protected BootstrapConfig bootstrapConfig;
	protected List<String> interestedSourceList;
	protected Map<String, String> tableUriToSrcNameMap;
	protected SchemaRegistryService schemaRegistryService;
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

	public void registerConsumer(SourceEventConsumer consumer)
	{
		this.sourceEventConsumer = consumer;
	}
}
