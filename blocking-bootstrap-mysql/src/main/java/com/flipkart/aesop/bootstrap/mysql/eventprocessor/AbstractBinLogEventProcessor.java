package com.flipkart.aesop.bootstrap.mysql.eventprocessor;

import java.util.ArrayList;
import java.util.List;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.event.AbstractEvent;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.schemas.VersionedSchema;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public abstract class AbstractBinLogEventProcessor implements BinLogEventProcessor
{
	public static final Logger LOGGER = LogFactory.getLogger(AbstractBinLogEventProcessor.class);

	protected List<AbstractEvent> map(long tableId, List<Row> rowList, OpenReplicationListener listener)
	{
		String tableName = listener.getTableIdtoNameMapping().get(tableId);
		return map(tableName, rowList, listener);
	}

	protected List<AbstractEvent> map(String tableName, List<Row> rowList,
	        OpenReplicationListener openReplicationListener)
	{
		List<AbstractEvent> sourceEvents = new ArrayList<AbstractEvent>();

		try
		{
			if (openReplicationListener.getInterestedSourceList().contains(tableName))
			{
				VersionedSchema schema =
				        openReplicationListener.getSchemaRegistryService().fetchLatestVersionedSchemaBySourceName(
				                openReplicationListener.getTableUriToSrcNameMap().get(tableName));

				for (Row row : rowList)
				{
					AbstractEvent abstractEvent =
					        openReplicationListener.getBinLogEventMapper().mapBinLogEvent(row, schema.getSchema(),
					                DbusOpcode.UPSERT);
					sourceEvents.add(abstractEvent);
				}
			}
			else
			{
				LOGGER.info("Event received form uninterested source");
			}
		}
		catch (Exception ex)
		{
			LOGGER.error("Exception : ", ex);
		}

		return sourceEvents;
	}
}
