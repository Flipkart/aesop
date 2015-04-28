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
package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.QueryEvent;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

/**
 * The <code>QueryEventProcessor</code> processes QueryEvent from source.
 * This event is received when transaction begins, ends, rollsback or any other sql query is run on the source.
 * But this processor would process only those sqls which causes mutation. All other sql queries are ignored.
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public class MutatingQueryEventProcessor implements BinLogEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(MutatingQueryEventProcessor.class);

	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener) throws Exception
	{
		QueryEvent queryEvent = (QueryEvent) event;
		String sql = queryEvent.getSql().toString();
		if ("BEGIN".equalsIgnoreCase(sql))
		{
			listener.getMysqlTransactionManager().setBeginTxnSeen(true);
			LOGGER.debug("BEGIN sql: " + sql);
			listener.getMysqlTransactionManager().startXtion();
		}
		else if ("COMMIT".equalsIgnoreCase(sql))
		{
			LOGGER.debug("COMMIT sql: " + sql);
			listener.getMysqlTransactionManager().endXtion(queryEvent.getHeader().getTimestamp());
		}
		else if ("ROLLBACK".equalsIgnoreCase(sql))
		{
			LOGGER.debug("ROLLBACK sql: " + sql);
			listener.getMysqlTransactionManager().resetTxn();
		}
		else
		{
			LOGGER.debug("unsupported DDL statement sql: " + sql);
		}
	}
}
