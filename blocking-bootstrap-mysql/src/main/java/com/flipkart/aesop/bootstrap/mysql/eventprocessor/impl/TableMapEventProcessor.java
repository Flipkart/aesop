package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.AbstractBinLogEventProcessor;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public class TableMapEventProcessor extends AbstractBinLogEventProcessor
{
	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener)
	{
		TableMapEvent tableMapEvent = (TableMapEvent) event;
		String newTableName =
		        tableMapEvent.getDatabaseName().toString().toLowerCase() + "."
		                + tableMapEvent.getTableName().toString().toLowerCase();
		Long newTableId = tableMapEvent.getTableId();
		listener.getTableIdtoNameMapping().put(newTableId, newTableName);
	}
}
