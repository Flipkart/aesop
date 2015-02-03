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

package com.flipkart.aesop.runtime.bootstrap;

import java.io.IOException;
import java.nio.ByteOrder;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.bootstrap.consumer.SourceEventConsumer;
import com.flipkart.aesop.runtime.bootstrap.producer.BlockingEventProducer;
import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdmin;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * Created by nikhil.bafna on 1/28/15.
 */
public class BlockingBootstrapServer extends ServerContainer
{
	public static final Logger LOGGER = LogFactory.getLogger(BlockingBootstrapServer.class);

	private BlockingEventProducer producer;
	private SourceEventConsumer consumer;

	public BlockingBootstrapServer(StaticConfig staticConfig) throws DatabusException, IOException
	{
		super(staticConfig, ByteOrder.BIG_ENDIAN);
	}

	@Override
	protected DatabusComponentAdmin createComponentAdmin()
	{
		return new DatabusComponentAdmin(this, getMbeanServer(), HttpRelay.class.getSimpleName());
	}

	@Override
	public void pause()
	{
		getComponentStatus().pause();
		producer.pause();
	}

	@Override
	public void resume()
	{
		getComponentStatus().resume();
		producer.unpause();
	}

	@Override
	public void suspendOnError(Throwable throwable)
	{
		getComponentStatus().suspendOnError(throwable);
	}

	@Override
	protected void doStart()
	{
		super.doStart();
		producer.registerConsumer(consumer);
		producer.start(0);
		this.registerShutdownHook();
	}

	@Override
	protected void doShutdown()
	{
		producer.shutdown();
		consumer.shutdown();
		super.doShutdown();
	}

	public void registerProducer(BlockingEventProducer producer)
	{
		this.producer = producer;
	}

	public void registerConsumer(SourceEventConsumer consumer)
	{
		this.consumer = consumer;
	}
}
