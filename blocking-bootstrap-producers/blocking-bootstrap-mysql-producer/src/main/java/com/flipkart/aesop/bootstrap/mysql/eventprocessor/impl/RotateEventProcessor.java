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
package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.bootstrap.mysql.txnprocessor.MysqlTransactionManager;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.RotateEvent;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

/**
 * The <code>QueryEventProcessor</code> processes RotateEvent from source. Rotate Event is called whenever the bin log file rotates.
 * Bin log file might get generated when the log size exceeds the configured size or log file is older than the configured number of days.
 * WARN:Log rotation happens even after mysql server restart. Currently log rotate event or stop event from master is not being received and
 * the same needs to be fixed.
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public class RotateEventProcessor implements BinLogEventProcessor
{

	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(RotateEventProcessor.class);

	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener) throws Exception {
		MysqlTransactionManager manager = listener.getMysqlTransactionManager();
		if ( !manager.isBeginTxnSeen()){
			LOGGER.warn("Skipping event (" + event
					+ ") as this is before the start of first transaction");
			return;
		}
		RotateEvent rotateEvent = (RotateEvent)event;
		String fileName = rotateEvent.getBinlogFileName().toString();
		LOGGER.info("File Rotated : FileName :" + fileName + ", BinlogFilePrefix :" + listener.getBinLogPrefix());
		String fileNumStr = fileName.substring(fileName.lastIndexOf(listener.getBinLogPrefix()) + listener.getBinLogPrefix().length() + 1);
		manager.setCurrFileNum(Integer.parseInt(fileNumStr));
	}
}
