package com.flipkart.aesop.consoleappenderdatalayer.upsert;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.consoleappenderdatalayer.delete.ConsoleAppenderDeleteDataLayer;
import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreOperation;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.core.DbusOpcode;

/**
 * Sample Upsert Data Layer. Persists {@link DbusOpcode#UPSERT} events to Logs.
 * @author Jagadeesh Huliyar
 * @see ConsoleAppenderDeleteDataLayer
 */
public class ConsoleAppenderUpsertDataLayer extends UpsertDestinationStoreOperation
{
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(ConsoleAppenderUpsertDataLayer.class);

	@Override
	protected void upsert(AbstractEvent event)
	{
		LOGGER.info("Received Upsert Event. Event is " + event);
	}
}
