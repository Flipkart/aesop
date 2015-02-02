package com.flipkart.aesop.runtime.bootstrap.consumer;

import java.util.ArrayList;
import java.util.List;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.eventconsumer.AbstractEventConsumer;
import com.google.common.base.Joiner;

public class DefaultBlockingEventConsumer implements SourceEventConsumer
{
	public static final Logger LOGGER = LogFactory.getLogger(DefaultBlockingEventConsumer.class);

	private final String PRIMARY_KEY_SEPERATOR = ";";
	private List<BoundedThreadPoolExecutor> executors = new ArrayList<BoundedThreadPoolExecutor>();
	private final int numberOfPartition;
	private final AbstractEventConsumer eventConsumer;

	public DefaultBlockingEventConsumer(int numberOfPartition, int executorQueueSize,
	        AbstractEventConsumer eventConsumer)
	{
		this.eventConsumer = eventConsumer;
		this.numberOfPartition = Math.min(numberOfPartition, Runtime.getRuntime().availableProcessors());

		LOGGER.info("numberOfPartition used: " + numberOfPartition);
		for (int i = 0; i < numberOfPartition; i++)
		{
			executors.add(new BoundedThreadPoolExecutor(1, executorQueueSize));
		}
	}

	@Override
	public void onEvent(AbstractEvent sourceEvent)
	{
		/** partition and submit */
		String primaryKeyValues = Joiner.on(PRIMARY_KEY_SEPERATOR).join(sourceEvent.getPrimaryKeySet());
		Integer partitionNumber = ((primaryKeyValues.hashCode() & 0x7fffffff) % numberOfPartition);
		LOGGER.debug("Partition:" + primaryKeyValues.hashCode() + ":" + partitionNumber);
		executors.get(partitionNumber).submit(new SourceEventProcessor(sourceEvent, eventConsumer));
	}

	public void shutdown() throws InterruptedException
	{
		for (int i = 0; i < numberOfPartition; i++)
		{
			executors.get(i).shutdown();
		}
	}
}
