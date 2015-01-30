package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import java.util.List;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.AbstractBinLogEventProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public class InsertEventProcessor extends AbstractBinLogEventProcessor
{
	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener)
	{
		WriteRowsEvent wre = (WriteRowsEvent) event;
		List<AbstractEvent> sourceEvents = map(wre.getTableId(), wre.getRows(), listener);
		for (AbstractEvent sourceEvent : sourceEvents)
		{
			listener.getSourceEventConsumer().onEvent(sourceEvent);
		}
	}
}
