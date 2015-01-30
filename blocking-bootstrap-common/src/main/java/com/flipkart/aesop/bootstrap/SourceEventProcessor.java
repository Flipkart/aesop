package com.flipkart.aesop.bootstrap;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.eventconsumer.AbstractEventConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;

/**
 * Created by nikhil.bafna on 1/25/15.
 */
public class SourceEventProcessor implements Runnable
{
	public static final Logger LOGGER = LogFactory.getLogger(DefaultBlockingEventConsumer.class);

	private final AbstractEvent sourceEvent;
	private final AbstractEventConsumer consumer;

	public SourceEventProcessor(AbstractEvent sourceEvent, AbstractEventConsumer consumer)
	{
		this.sourceEvent = sourceEvent;
		this.consumer = consumer;
	}

	public void run()
	{
		try
		{
			LOGGER.info("Processing :" + sourceEvent.getPrimaryKeyValues() + ":" + sourceEvent.getNamespaceName() + "."
			        + sourceEvent.getEntityName());
			ConsumerCallbackResult consumerCallbackResult = consumer.processSourceEvent(sourceEvent);
			LOGGER.info(consumerCallbackResult.toString());
		}
		catch (Exception e)
		{
			LOGGER.error("Exception occurred while processing event " + e.getMessage(), e);
		}
	}

}
