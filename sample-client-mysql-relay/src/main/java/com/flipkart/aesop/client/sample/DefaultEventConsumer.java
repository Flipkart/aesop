package com.flipkart.aesop.client.sample;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.events.ortest.DefaultBinLogEvent;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus2.core.DatabusException;

/**
 * yogesh.dahiya
 */

public class DefaultEventConsumer extends AbstractDatabusCombinedConsumer
{
	/** Logger for this class */
	public static final Logger LOGGER = LogFactory.getLogger(DefaultEventConsumer.class);

	/** The frequency of logging consumed messages */
	private static final long FREQUENCY_OF_LOGGING = 1;

	/** The event count */
	private long eventCount = 0;

	/**
	 * Overriden superclass method. Returns the result of calling
	 * {@link DefaultEventConsumer#processEvent(DbusEvent, DbusEventDecoder)}
	 * @see com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer#onDataEvent(com.linkedin.databus.core.DbusEvent,
	 *      com.linkedin.databus.client.pub.DbusEventDecoder)
	 */
	public ConsumerCallbackResult onDataEvent(DbusEvent event, DbusEventDecoder eventDecoder)
	{
		return processEvent(event, eventDecoder);
	}

	/**
	 * Overriden superclass method. Returns the result of calling
	 * {@link DefaultEventConsumer#processEvent(DbusEvent, DbusEventDecoder)}
	 * @see com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer#onBootstrapEvent(com.linkedin.databus.core.DbusEvent,
	 *      com.linkedin.databus.client.pub.DbusEventDecoder)
	 */
	public ConsumerCallbackResult onBootstrapEvent(DbusEvent event, DbusEventDecoder eventDecoder)
	{
		return processEvent(event, eventDecoder);
	}

	/**
	 * Helper method that prints out the attributes of the change event.
	 * @param event the Databus change event
	 * @param eventDecoder the Event decoder
	 * @return {@link ConsumerCallbackResult#SUCCESS} if successful and {@link ConsumerCallbackResult#ERROR} in case of
	 *         exceptions/errors
	 */
	private ConsumerCallbackResult processEvent(DbusEvent event, DbusEventDecoder eventDecoder)
	{

		LOGGER.info("Source Id is " + event.getSourceId());
		try
		{
			DefaultBinLogEvent genericBinLogEvent = new DefaultBinLogEvent(event, eventDecoder);
			LOGGER.info(genericBinLogEvent.getKeyValuePair().toString());
			LOGGER.info(genericBinLogEvent.getPrimaryKeyList().toString());
		}
		catch (DatabusException ex)
		{
			LOGGER.error("error in consuming events", ex);
			return ConsumerCallbackResult.ERROR;
		}
		eventCount++;
		return ConsumerCallbackResult.SUCCESS;
	}
}
