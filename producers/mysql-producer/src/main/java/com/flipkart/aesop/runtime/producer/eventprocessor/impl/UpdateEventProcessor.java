/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.runtime.producer.eventprocessor.impl;

import java.util.ArrayList;
import java.util.List;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.runtime.producer.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.runtime.producer.txnprocessor.MysqlTransactionManager;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;

/**
 * The <code>UpdateEventProcessor</code> processes UpdateRowsEvent from source. This event is received if there is any
 * update operation on the source.
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public class UpdateEventProcessor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(UpdateEventProcessor.class);

	/**
	 * @see com.flipkart.aesop.runtime.producer.eventprocessor.BinLogEventProcessor#process(com.google.code.or.binlog.BinlogEventV4,
	 *      com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener)
	 */
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
		UpdateRowsEvent updateRowsEvent = (UpdateRowsEvent) event;
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
