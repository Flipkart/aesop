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

package com.flipkart.aesop.bootstrap.mysql;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.bootstrap.mysql.configs.OpenReplicatorConfig;
import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.runtime.bootstrap.producer.BlockingEventProducer;
import com.google.code.or.OpenReplicator;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public class MysqlEventProducer extends BlockingEventProducer
{
	public static final Logger LOGGER = LogFactory.getLogger(MysqlEventProducer.class);
	private static Long startTime = System.nanoTime();

	private OpenReplicatorConfig openReplicatorConfig;
	private Map<Integer, BinLogEventProcessor> eventProcessors;

	private OpenReplicator openReplicator = new OpenReplicator();

	@Override
	public void start(long l)
	{
		try
		{
			FileSystemSchemaRegistryService.Config configBuilder = new FileSystemSchemaRegistryService.Config();
			configBuilder.setFallbackToResources(true);
			configBuilder.setSchemaDir(bootstrapConfig.getSchemaRegistryLocation());
			FileSystemSchemaRegistryService.StaticConfig schemaRegistryServiceConfig = configBuilder.build();
			schemaRegistryService = FileSystemSchemaRegistryService.build(schemaRegistryServiceConfig);

			OpenReplicationListener orListener =
			        new OpenReplicationListener(openReplicatorConfig.getBinlogPrefix(),
			                openReplicatorConfig.getEndFileNumber(), interestedSourceList, tableUriToSrcNameMap,
			                schemaRegistryService, sourceEventConsumer, eventProcessors, this);

			String binlogFile =
			        String.format("%s.%06d", openReplicatorConfig.getBinlogPrefix(),
			                openReplicatorConfig.getStartFileNumber());

			openReplicator.setBinlogFileName(binlogFile);
			openReplicator.setBinlogPosition(openReplicatorConfig.getBinlogPosition());
			openReplicator.setBinlogEventListener(orListener);
			openReplicator.setUser(openReplicatorConfig.getUserName());
			openReplicator.setPassword(openReplicatorConfig.getPassword());
			openReplicator.setHost(openReplicatorConfig.getHost());
			openReplicator.setPort(openReplicatorConfig.getPort());
			openReplicator.setServerId(openReplicatorConfig.getServerId());

			openReplicator.start();
		}
		catch (Exception e)
		{
			LOGGER.error("Error occurred while starting open replication.", e);
		}
	}

	@Override
	public String getName()
	{
		return this.getClass().getName();
	}

	@Override
	public long getSCN()
	{
		return 0;
	}

	@Override
	public boolean isRunning()
	{
		return this.openReplicator.isRunning();
	}

	@Override
	public boolean isPaused()
	{
		return !this.openReplicator.isRunning();
	}

	@Override
	public void unpause()
	{
		throw new UnsupportedOperationException("'unpause' is not supported on this event producer");
	}

	@Override
	public void pause()
	{
		throw new UnsupportedOperationException("'unpause' is not supported on this event producer");
	}

	@Override
	public void shutdown()
	{
		try
		{
			LOGGER.info("Shutdown has been requested. MYSQLEventProducer shutting down");
			this.openReplicator.stop(5, TimeUnit.SECONDS);
			LOGGER.info("### Bootstrap Process completed successfully ###");
			LOGGER.info("Time Taken:" + (System.nanoTime() - startTime));
		}
		catch (Exception e)
		{
			LOGGER.error("Error while stopping mysql bootstrap", e);
		}
	}

	@Override
	public void waitForShutdown() throws InterruptedException, IllegalStateException
	{
		throw new UnsupportedOperationException("'waitForShutdown' is not supported on this event producer");
	}

	@Override
	public void waitForShutdown(long l) throws InterruptedException, IllegalStateException
	{
		throw new UnsupportedOperationException("'waitForShutdown' is not supported on this event producer");
	}

	public OpenReplicatorConfig getOpenReplicatorConfig()
	{
		return openReplicatorConfig;
	}

	public void setOpenReplicatorConfig(OpenReplicatorConfig openReplicatorConfig)
	{
		this.openReplicatorConfig = openReplicatorConfig;
	}

	public Map<Integer, BinLogEventProcessor> getEventProcessors()
	{
		return eventProcessors;
	}

	public void setEventProcessors(Map<Integer, BinLogEventProcessor> eventProcessors)
	{
		this.eventProcessors = eventProcessors;
	}

}
