package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.bootstrap.mysql.txnprocessor.MysqlTransactionManager;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * The <code>UpdateEvent2Processor</code> processes UpdateRowsEventV2 from source. This event is received if there is
 * any
 * update operation on the source.
 * @author jagadeesh.huliyar
 */
public class UpdateEventV2Processor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(UpdateEventV2Processor.class);

	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener) throws Exception
	{
		MysqlTransactionManager manager = listener.getMysqlTransactionManager();
		if (!manager.isBeginTxnSeen())
		{
			LOGGER.warn("Skipping event (" + event + ") as this is before the start of first transaction");
			return;
		}
		LOGGER.debug("Update Event Received : " + event);
		UpdateRowsEventV2 updateRowsEvent = (UpdateRowsEventV2) event;
		List<Pair<Row>> listOfPairs = updateRowsEvent.getRows();
		List<Row> rowList = new ArrayList<Row>(listOfPairs.size());
		for (Pair<Row> pair : listOfPairs)
		{
			Row row = pair.getAfter();
			rowList.add(row);
		}
		manager.performChanges(updateRowsEvent.getTableId(), updateRowsEvent.getHeader(), rowList, DbusOpcode.UPSERT);
		LOGGER.debug("Update Successful for  " + event.getHeader().getEventLength() + " . Data updated : " + rowList);
	}
}
