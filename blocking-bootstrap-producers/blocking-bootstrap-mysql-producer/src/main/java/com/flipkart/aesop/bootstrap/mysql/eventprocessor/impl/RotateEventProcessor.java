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

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.RotateEvent;

/**
 * The <code>RotateEventProcessor</code> processes RotateEvent from source. Rotate Event is called whenever the bin log
 * file rotates. Bin log file gets rotated when the log file size exceeds the configured size or log file is older than
 * the configured number of days.
 * @author nrbafna
 */
public class RotateEventProcessor<T extends AbstractEvent> implements BinLogEventProcessor<T>
{
	public static final Logger LOGGER = LogFactory.getLogger(RotateEventProcessor.class);

	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener<T> listener)
	{
		String binlogPrefix = listener.getBinlogPrefix();
		RotateEvent rotateEvent = (RotateEvent) event;
		String fileName = rotateEvent.getBinlogFileName().toString();
		LOGGER.info("File Rotated : FileName :" + fileName);

		String fileNumStr = fileName.substring(fileName.lastIndexOf(binlogPrefix) + binlogPrefix.length() + 1);
		Long fileNumber = Long.parseLong(fileNumStr);

		LOGGER.info("BinlogFile Number : " + fileNumber);
		if (fileNumber.equals(listener.getEndFileNum()))
		{
			LOGGER.info("Bootstrap Process completed successfully !!!");
			listener.getSourceEventConsumer().shutdown();
			listener.shutdown();
		}
	}
}
