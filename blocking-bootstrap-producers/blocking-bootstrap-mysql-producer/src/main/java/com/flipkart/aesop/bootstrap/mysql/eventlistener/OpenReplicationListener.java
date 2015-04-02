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

package com.flipkart.aesop.bootstrap.mysql.eventlistener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.bootstrap.mysql.MysqlEventProducer;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.bootstrap.mysql.mapper.BinLogEventMapper;
import com.flipkart.aesop.bootstrap.mysql.mapper.impl.DefaultBinLogEventMapper;
import com.flipkart.aesop.bootstrap.mysql.utils.ORToMysqlMapper;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.runtime.bootstrap.consumer.SourceEventConsumer;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * The <OpenReplicationListener> is a binary log callback implementation of {@link BinlogEventListener}.
 * <a href="https://code.google.com/p/open-replicator/">OpenReplicator</a> provides callback to this implementation.
 * @author nrbafna
 */
public class OpenReplicationListener<T extends AbstractEvent> implements BinlogEventListener
{
	public static final Logger LOGGER = LogFactory.getLogger(OpenReplicationListener.class);

	private String binlogPrefix;
	private Long endFileNum;
	private SchemaRegistryService schemaRegistryService;
	private List<String> interestedSourceList;
	private Map<String, String> tableUriToSrcNameMap;
	private BinLogEventMapper<T> binLogEventMapper;
	private SourceEventConsumer sourceEventConsumer;
	private Map<Integer, BinLogEventProcessor<T>> processors;
	private EventProducer shutdownListener;

	private Map<Long, String> tableIdtoNameMapping = new HashMap<Long, String>();

	public OpenReplicationListener(String binlogPrefix, Long endFileNum, List<String> interestedSourceList,
	        Map<String, String> tableUriToSrcNameMap, SchemaRegistryService schemaRegistryService,
	        SourceEventConsumer sourceEventConsumer, Map<Integer, BinLogEventProcessor<T>> eventProcessorMap,
	        MysqlEventProducer<T> mysqlEventProducer)
	{
		this.binlogPrefix = binlogPrefix;
		this.endFileNum = endFileNum;
		this.schemaRegistryService = schemaRegistryService;
		this.tableUriToSrcNameMap = tableUriToSrcNameMap;
		this.interestedSourceList = interestedSourceList;
		this.binLogEventMapper = new DefaultBinLogEventMapper<T>(new ORToMysqlMapper());
		this.sourceEventConsumer = sourceEventConsumer;
		this.processors = eventProcessorMap;
		this.shutdownListener = mysqlEventProducer;
	}

	public void onEvents(BinlogEventV4 event)
	{
		if (event == null)
		{
			LOGGER.error("Received null event");
			return;
		}
		LOGGER.info("Current SCN:" + event.getHeader().getPosition());

		int eventType = event.getHeader().getEventType();
		BinLogEventProcessor<T> processor = processors.get(eventType);
		if (processor != null)
		{
			processor.process(event, this);
		}
		else
		{
			LOGGER.warn("Ignoring Unsupported Event! " + event.getHeader().getEventType());
		}
	}

	public void shutdown()
	{
		shutdownListener.shutdown();
	}

	public String getBinlogPrefix()
	{
		return binlogPrefix;
	}

	public Long getEndFileNum()
	{
		return endFileNum;
	}

	public SchemaRegistryService getSchemaRegistryService()
	{
		return schemaRegistryService;
	}

	public List<String> getInterestedSourceList()
	{
		return interestedSourceList;
	}

	public Map<String, String> getTableUriToSrcNameMap()
	{
		return tableUriToSrcNameMap;
	}

	public Map<Long, String> getTableIdtoNameMapping()
	{
		return tableIdtoNameMapping;
	}

	public BinLogEventMapper<T> getBinLogEventMapper()
	{
		return binLogEventMapper;
	}

	public SourceEventConsumer getSourceEventConsumer()
	{
		return sourceEventConsumer;
	}
}
