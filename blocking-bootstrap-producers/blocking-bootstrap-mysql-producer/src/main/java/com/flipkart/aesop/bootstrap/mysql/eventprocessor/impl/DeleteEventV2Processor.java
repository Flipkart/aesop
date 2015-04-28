package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.linkedin.databus.core.DbusOpcode;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

/**
 * The <code>DeleteEventV2Processor</code> processes DeleteRowsEventV2 from source. This event gets called when ever few
 * row/(s) are deleted at the source.
 * @author jagadeesh.huliyar
 */
public class DeleteEventV2Processor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(DeleteEventV2Processor.class);

	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener) throws Exception
	{
		if (!listener.getMysqlTransactionManager().isBeginTxnSeen())
		{
			LOGGER.warn("Skipping event (" + event + ") as this is before the start of first transaction");
			return;
		}
		LOGGER.debug("Delete Event Received : " + event);
		DeleteRowsEventV2 deleteRowsEvent = (DeleteRowsEventV2) event;
		listener.getMysqlTransactionManager().performChanges(deleteRowsEvent.getTableId(), deleteRowsEvent.getHeader(),
		        deleteRowsEvent.getRows(), DbusOpcode.DELETE);
		LOGGER.debug("Delete Successful for  " + event.getHeader().getEventLength() + " . Data deleted : "
		        + deleteRowsEvent.getRows());
	}
}
