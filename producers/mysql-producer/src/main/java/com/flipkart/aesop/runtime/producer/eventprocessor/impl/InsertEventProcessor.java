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

import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.runtime.producer.eventprocessor.BinLogEventProcessor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.linkedin.databus.core.DbusOpcode;

import java.util.ArrayList;
import java.util.List;

/**
 * The <code>InsertEventProcessor</code> processes WriteRowsEvent from source. This event is received whenever insertion
 * operation happens at the source.
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public class InsertEventProcessor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(InsertEventProcessor.class);

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
		WriteRowsEvent wre = (WriteRowsEvent) event;
		List<Row> rowList = wre.getRows();
		List<Pair<Row>> listOfPairs = new ArrayList<Pair<Row>>(rowList.size());

		for (Row row : rowList)
		{
			//Inserting Old Row as null
			Pair rowPair = new Pair(null, row);
			listOfPairs.add(rowPair);
		}

		listener.getMysqlTransactionManager().performChanges(wre.getTableId(), wre.getHeader(), listOfPairs,
				DbusOpcode.UPSERT);
		LOGGER.debug("Insertion Successful for  " + event.getHeader().getEventLength() + " . Data inserted : "
		        + rowList);
	}

}
