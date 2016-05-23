package com.flipkart.aesop.runtime.producer.eventprocessor.impl;

import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.runtime.producer.eventprocessor.BinLogEventProcessor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.linkedin.databus.core.DbusOpcode;

import java.util.ArrayList;
import java.util.List;

/**
 * The <code>DeleteEventV2Processor</code> processes DeleteRowsEventV2 from source. This event gets called when ever few
 * row/(s) are deleted at the source.
 * @author jagadeesh.huliyar
 */
public class DeleteEventV2Processor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(DeleteEventV2Processor.class);

	/**
	 * @see com.flipkart.aesop.runtime.producer.eventprocessor.BinLogEventProcessor#process(com.google.code.or.binlog.BinlogEventV4,
	 *      com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener)
	 */
	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener) throws Exception
	{
		if (!listener.getMysqlTransactionManager().isBeginTxnSeen())
		{
			LOGGER.warn("Skipping event ({}) as this is before the start of first transaction", event);
			return;
		}
		LOGGER.debug("Delete Event Received : {}", event);
		DeleteRowsEventV2 deleteRowsEvent = (DeleteRowsEventV2) event;
		List<Row> rowList = deleteRowsEvent.getRows();
		List<Pair<Row>> listOfPairs = new ArrayList<Pair<Row>>(rowList.size());

		for (Row row : rowList)
		{
			/* null is added in before to maintain consistency between with update and further in code we dont need to
			 *differentiate update and delete */
			listOfPairs.add(new Pair<Row>(null, row));
		}

		listener.getMysqlTransactionManager().performChanges(deleteRowsEvent.getTableId(), deleteRowsEvent.getHeader(),
				listOfPairs, DbusOpcode.DELETE);
		LOGGER.debug("Delete Successful for  {} . Data deleted : {}", event.getHeader().getEventLength(), rowList);
	}
}
