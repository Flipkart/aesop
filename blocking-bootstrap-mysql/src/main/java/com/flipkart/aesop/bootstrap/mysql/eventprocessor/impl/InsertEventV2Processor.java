package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import java.util.List;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.AbstractBinLogEventProcessor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public class InsertEventV2Processor extends AbstractBinLogEventProcessor
{
	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener)
	{
		WriteRowsEventV2 wre = (WriteRowsEventV2) event;
		List<AbstractEvent> sourceEvents = map(wre.getTableId(), wre.getRows(), listener);
		for (AbstractEvent sourceEvent : sourceEvents)
		{
			listener.getSourceEventConsumer().onEvent(sourceEvent);
		}
	}
}
