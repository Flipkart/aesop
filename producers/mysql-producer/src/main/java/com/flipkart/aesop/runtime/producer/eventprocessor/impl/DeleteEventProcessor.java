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

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.runtime.producer.eventprocessor.BinLogEventProcessor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.linkedin.databus.core.DbusOpcode;

/**
 * The <code>DeleteEventProcessor</code> processes DeleteRowsEvent from source. This event gets called when ever few
 * row/(s) are deleted at the source.
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public class DeleteEventProcessor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(DeleteEventProcessor.class);

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
		LOGGER.debug("Delete Event Received : " + event);
		DeleteRowsEvent deleteRowsEvent = (DeleteRowsEvent) event;
		listener.getMysqlTransactionManager().performChanges(deleteRowsEvent.getTableId(), deleteRowsEvent.getHeader(),
		        deleteRowsEvent.getRows(), DbusOpcode.DELETE);
		LOGGER.debug("Delete Successful for  " + event.getHeader().getEventLength() + " . Data deleted : "
		        + deleteRowsEvent.getRows());
	}
}
