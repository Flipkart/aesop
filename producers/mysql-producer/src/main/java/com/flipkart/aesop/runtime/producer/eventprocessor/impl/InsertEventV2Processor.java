package com.flipkart.aesop.runtime.producer.eventprocessor.impl;

import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.runtime.producer.eventprocessor.BinLogEventProcessor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.linkedin.databus.core.DbusOpcode;

import java.util.ArrayList;
import java.util.List;

/**
 * The <code>InsertEvent2Processor</code> processes WriteRowsEventV2 from source. This event is received if there is any
 * insert operation on the source.
 * @author jagadeesh.huliyar
 */
public class InsertEventV2Processor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(InsertEventV2Processor.class);

	/**
	 * @see com.flipkart.aesop.runtime.producer.eventprocessor.BinLogEventProcessor#process(com.google.code.or.binlog.BinlogEventV4,
	 *      com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener)
	 */
	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener) throws Exception
	{
		if (!listener.getMysqlTransactionManager().isBeginTxnSeen())
		{
			LOGGER.warn("Skipping event (" + event + ") as this is before the start of first transaction");
			return;
		}
		LOGGER.debug("Insert Event Received : " + event);
		WriteRowsEventV2 wre = (WriteRowsEventV2) event;
		List<Row> rowList = wre.getRows();
		List<Pair<Row>> listOfPairs = new ArrayList<Pair<Row>>(rowList.size());

		for (Row row : rowList)
		{
			listOfPairs.add(new Pair<Row>(null, row));
		}

		listener.getMysqlTransactionManager().performChanges(wre.getTableId(), wre.getHeader(), listOfPairs,
				DbusOpcode.UPSERT);
		LOGGER.debug("Insertion Successful for  " + event.getHeader().getEventLength() + " . Data inserted : "
				+ rowList);
	}

}
