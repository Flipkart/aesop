package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.RotateEvent;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public class RotateEventProcessor implements BinLogEventProcessor
{
	public static final Logger LOGGER = LogFactory.getLogger(RotateEventProcessor.class);

	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener)
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
			listener.shutdown();
		}
	}
}
