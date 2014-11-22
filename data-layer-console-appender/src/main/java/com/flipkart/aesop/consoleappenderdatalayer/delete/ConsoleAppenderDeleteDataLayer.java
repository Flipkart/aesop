package com.flipkart.aesop.consoleappenderdatalayer.delete;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.consoleappenderdatalayer.upsert.ConsoleAppenderUpsertDataLayer;
import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreOperation;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.core.DbusOpcode;

/**
 * Sample Delete Data Layer. Persists {@link DbusOpcode#DELETE} events to Log File.
 * @author Jagadeesh Huliyar
 * @see ConsoleAppenderUpsertDataLayer
 */
public class ConsoleAppenderDeleteDataLayer extends DeleteDestinationStoreOperation
{
	
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(ConsoleAppenderDeleteDataLayer.class);

	@Override
	protected void delete(AbstractEvent event)
	{
		LOGGER.info("Received Delete Event. Event is " + event);
	}

}
