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

package com.flipkart.aesop.runtime.bootstrap.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.eventconsumer.AbstractEventConsumer;
import com.google.common.base.Joiner;

/**
 * <code>DefaultBlockingEventConsumer</code> is the default blocking implementation for {@link SourceEventConsumer}. It
 * submits the events to the appropriate thread pool.
 * @author nrbafna
 */
public class DefaultBlockingEventConsumer implements SourceEventConsumer
{
	public static final Logger LOGGER = LogFactory.getLogger(DefaultBlockingEventConsumer.class);

	private final String PRIMARY_KEY_SEPERATOR = ";";
	private List<ThreadPoolExecutor> executors = new ArrayList<ThreadPoolExecutor>();
	private final int numberOfPartition;
	private final AbstractEventConsumer eventConsumer;

	public DefaultBlockingEventConsumer(int numberOfPartition, int executorQueueSize,
	        AbstractEventConsumer eventConsumer, RejectedExecutionHandler rejectedExecutionHandler)
	{
		this.eventConsumer = eventConsumer;
		this.numberOfPartition = Math.min(numberOfPartition, Runtime.getRuntime().availableProcessors());

		LOGGER.info("numberOfPartition used: " + numberOfPartition);
		for (int i = 0; i < numberOfPartition; i++)
		{
			BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(executorQueueSize);
			executors.add(new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue, rejectedExecutionHandler));
		}
	}

	@Override
	public void onEvent(AbstractEvent sourceEvent)
	{
		/** partition and submit */
		String primaryKeyValues = Joiner.on(PRIMARY_KEY_SEPERATOR).join(sourceEvent.getPrimaryKeyValues());
		Integer partitionNumber = ((primaryKeyValues.hashCode() & 0x7fffffff) % numberOfPartition);
		LOGGER.debug("Partition:" + primaryKeyValues.hashCode() + ":" + partitionNumber);
		executors.get(partitionNumber).execute(new SourceEventProcessor(sourceEvent, eventConsumer));
	}

	public void shutdown()
	{
		for (int i = 0; i < numberOfPartition; i++)
		{
			executors.get(i).shutdown();
		}

		try
		{
			for (int i = 0; i < numberOfPartition; i++)
			{
				executors.get(i).awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			}
		}
		catch (InterruptedException e)
		{
			LOGGER.error("Error while stopping bootstrap consumer", e);
		}
	}
}
