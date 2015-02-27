/**
 * @author jagadeesh.huliyar
 */
package com.flipkart.aesop.sample.client.common.consumer;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.sample.client.common.events.MysqlBinLogEvent;
import com.flipkart.aesop.sample.client.common.events.MysqlBinLogEventImpl;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus2.core.DatabusException;

public abstract class AbstractMySqlEventConsumer extends AbstractDatabusCombinedConsumer
{
	/** Logger for this class */
	public static final Logger LOGGER = LogFactory.getLogger(AbstractMySqlEventConsumer.class);

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

		LOGGER.debug("Source Id is " + event.getSourceId());
		MysqlBinLogEvent mysqlBinLogEvent = null;
		try
		{
			mysqlBinLogEvent = new MysqlBinLogEventImpl(event, eventDecoder);
			LOGGER.info("Event : " + mysqlBinLogEvent.toString());
		}
		catch (DatabusException ex)
		{
			LOGGER.error("error in consuming events", ex);
			return ConsumerCallbackResult.ERROR;
		}
		return processEvent(mysqlBinLogEvent);
	}
	
	public abstract ConsumerCallbackResult processEvent(MysqlBinLogEvent mysqlBinLogEvent);
}
