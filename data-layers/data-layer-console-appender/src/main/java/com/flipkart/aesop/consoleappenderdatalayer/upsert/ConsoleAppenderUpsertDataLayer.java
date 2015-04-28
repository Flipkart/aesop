package com.flipkart.aesop.consoleappenderdatalayer.upsert;

import com.flipkart.aesop.consoleappenderdatalayer.delete.ConsoleAppenderDeleteDataLayer;
import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusOpcode;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

/**
 * Sample Upsert Data Layer. Persists {@link DbusOpcode#UPSERT} events to Logs.
 * @author Jagadeesh Huliyar
 * @see ConsoleAppenderDeleteDataLayer
 */
public class ConsoleAppenderUpsertDataLayer extends UpsertDestinationStoreProcessor
{
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(ConsoleAppenderUpsertDataLayer.class);

	@Override
	protected ConsumerCallbackResult upsert(AbstractEvent event)
	{
		LOGGER.info("DESTINATION_EVENT:UPSERT: Event is " + event);
        return ConsumerCallbackResult.SUCCESS;
	}
}
