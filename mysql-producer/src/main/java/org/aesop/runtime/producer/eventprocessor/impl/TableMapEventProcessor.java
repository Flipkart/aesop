/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aesop.runtime.producer.eventprocessor.impl;

import org.aesop.runtime.producer.eventlistener.OpenReplicationListener;
import org.aesop.runtime.producer.eventprocessor.BinLogEventProcessor;
import org.aesop.runtime.producer.txnprocessor.MysqlTransactionManager;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.linkedin.databus.core.DatabusRuntimeException;

/**
 * The <code>TableMapEventProcessor</code> processes TableMapEvent from source. This event gives table related details for the started transaction. 
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public class TableMapEventProcessor implements BinLogEventProcessor{
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(TableMapEventProcessor.class);

	/**
	 * @see org.aesop.runtime.producer.eventprocessor.BinLogEventProcessor#process(com.google.code.or.binlog.BinlogEventV4, org.aesop.runtime.producer.eventlistener.OpenReplicationListener)
	 */
	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener) throws Exception {
		MysqlTransactionManager manager = listener.getMysqlTransactionManager();
		if ( !manager.isBeginTxnSeen()){
			LOGGER.warn("Skipping event (" + event
					+ ") as this is before the start of first transaction");
			return;
		}
		TableMapEvent tableMapEvent = (TableMapEvent) event;
		String newTableName = tableMapEvent.getDatabaseName().toString().toLowerCase() + "." + tableMapEvent.getTableName().toString().toLowerCase();
		long newTableId = tableMapEvent.getTableId();
		String curTableName=manager.getCurrTableName();
		long curTableId=manager.getCurrTableId();

		final boolean areTableNamesEqual = curTableName.equals(newTableName);
		final boolean areTableIdsEqual = ( curTableId== newTableId);
		final boolean didTableNameChange = !(areTableNamesEqual && areTableIdsEqual);

		if (curTableName.isEmpty() && (curTableId == -1)){
			/**First TableMapEvent for the transaction. Indicates the first event in the transaction is yet to come*/
			manager.startSource(newTableName, newTableId);
		}else if (didTableNameChange){
			LOGGER.debug("Table name changed from " + curTableName + " to " + newTableName);
			/** Event will come for a new source. Invoke an endSource on currTableName, and a startSource on newTableName*/
			manager.endSource();
			manager.startSource(newTableName, newTableId);
		}else{
			LOGGER.error("Unexpected : TableMap Event obtained :" + tableMapEvent);
			throw new DatabusRuntimeException("Unexpected : TableMap Event obtained :" +
					" _currTableName = " + curTableName +
					" _curTableId = " + curTableId +
					" newTableName = " + newTableName +
					" newTableId = " + newTableId);
		}
	}
}
