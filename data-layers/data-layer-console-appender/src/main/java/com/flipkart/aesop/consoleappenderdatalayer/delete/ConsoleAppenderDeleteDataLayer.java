package com.flipkart.aesop.consoleappenderdatalayer.delete;

import com.flipkart.aesop.consoleappenderdatalayer.upsert.ConsoleAppenderUpsertDataLayer;
import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusOpcode;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

/**
 * Sample Delete Data Layer. Persists {@link DbusOpcode#DELETE} events to Log File.
 * @author Jagadeesh Huliyar
 * @see ConsoleAppenderUpsertDataLayer
 */
public class ConsoleAppenderDeleteDataLayer extends DeleteDestinationStoreProcessor
{
	
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(ConsoleAppenderDeleteDataLayer.class);

	@Override
	protected  ConsumerCallbackResult delete(AbstractEvent event)
	{
		LOGGER.info("DESTINATION_EVENT:DELETE:. Event is " + event);

        return ConsumerCallbackResult.SUCCESS;
	}

}
