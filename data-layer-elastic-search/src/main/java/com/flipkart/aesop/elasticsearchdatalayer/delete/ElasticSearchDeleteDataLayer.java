package com.flipkart.aesop.elasticsearchdatalayer.delete;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreOperation;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.core.DbusOpcode;

import com.flipkart.aesop.elasticsearchdatalayer.DialServer;



/**
 * Sample Delete Data Layer. Persists {@link DbusOpcode#DELETE} events to Log File.
 * @author Jagadeesh Huliyar
 * @see com.flipkart.aesop.elasticsearchdatalayer.upsert.ElasticSearchUpsertDataLayer
 */
public class ElasticSearchDeleteDataLayer extends DeleteDestinationStoreOperation
{
	
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchDeleteDataLayer.class);
    private static final DialServer ds = new DialServer();

	@Override
	protected void delete(AbstractEvent event)
	{
        ds.checkServer();
		LOGGER.info("Received Delete Event. Event is " + event);
	}

}
